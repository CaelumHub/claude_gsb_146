"""服务端撤销历史 —— 按「白板 × 用户」持久化撤销/重做栈。

为什么撤销历史要放服务端
------------------------
旧实现的撤销栈只在浏览器内存里: 刷新页面、断线重连或换一台设备登录后,
用户就无法撤销自己最近的操作。本模块把每个用户在每个白板上的撤销栈
落盘到 ``boards/<bid>/undo/<user>.json``, 任何设备连上后由 welcome
带回栈摘要(仅 gid/label, 不含操作体), 撤销/重做在服务端签发。

为什么撤销不会破坏与他人并发编辑的合并
--------------------------------------
撤销**不是回滚状态**, 而是发布一条带*全新* op_id 与 lamport 时钟的
语义逆操作(move→反向增量, add→delete, set_props→写回旧值……),
逆操作与普通编辑走完全相同的 CRDT 接入/广播路径:

* move 的逆仍是可交换的增量, 与任何人的并发位移相加合成;
* delete/restore/add 走 deleted 字段 LWW, 乱序/延迟都收敛;
* set_props/reorder/reparent 等字段级 LWW 逆操作, 在签发前会扫描
  该撤销单元 ``base_rev`` 之后**其他用户**留下的操作: 一旦某字段
  已被他人并发写过, 逆操作就跳过该字段(见 ``plan_inverse``),
  因此不会用旧值覆盖他人的并发修改;
* 逆操作由服务端而不是客户端签发, 多设备/重连重复请求按 req_id
  幂等去重, 不会二次生效。

存储格式(单用户单文件, 原子写):
    {"v": 1,
     "undo": [{"gid","label","ts","base_rev","undo":[叶子], "redo":[叶子]}],
     "redo": [...]}

叶子只保留语义负载(无 op_id/lam 等信封字段); 撤销时由本模块用
服务端站点与随机 op_id 重新签发, 避免与历史 op_id 撞幂等表。
"""
from __future__ import annotations

import asyncio
import os
import secrets
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set, Tuple

from . import config
from .crdt import _finite_number, now_ms, validate_op
from .storage import read_json, safe_id, write_json_atomic

UNDO = "undo"
REDO = "redo"
DIRECTIONS = (UNDO, REDO)

MAX_ENTRY_LEAVES = 512                # 单个撤销单元的叶子上限(与 batch 上限一致)
MAX_LABEL_LEN = 40
MAX_GID_LEN = 64
MAX_ENTRY_JSON_BYTES = 1_500_000      # 单条目序列化上限(手绘大笔迹兜底)
MAX_STATE_JSON_BYTES = 4_000_000      # 单用户撤销文件总上限, 超出从最旧条目修剪
FOREIGN_SCAN_LIMIT = 5000             # 逆操作前扫描他人操作的上限, 超出则保守跳过

#: 叶子上属于「信封」、入库前剥除的字段
ENVELOPE_KEYS = ("op_id", "site", "lam", "ts", "base_rev", "rev", "by", "dup")

#: 签发逆操作的服务端站点: 进程内唯一, 跨进程靠 op_id 随机后缀去重
SERVER_SITE = f"srvundo-{secrets.token_hex(4)}"

#: 逆操作叶子可能携带的 payload 字段(白名单, 与 validate_op 对齐)
_PAYLOAD_KEYS = {
    "add_shape": ("shape",),
    "delete_shape": ("id",),
    "restore_shape": ("id",),
    "move": ("id", "dx", "dy"),
    "set_props": ("id", "props"),
    "reorder": ("id", "z"),
    "reparent": ("id", "parent"),
    "path_extend": ("id", "points"),
    "truncate_path": ("id", "length"),
}


# ---------------------------------------------------------------- 叶子清洗
def clean_leaf(raw: Any) -> Optional[Dict[str, Any]]:
    """把客户端 checkpoint 里的一个语义叶子清洗为无信封的规范形式。

    走与普通操作相同的 validate_op(套一个假信封), 保证撤销历史里
    不会藏入任何普通操作管线拒绝的内容; move 额外放行「单轴」增量
    (拖动手势可能只有 x 或 y 一个方向, 普通操作管线要求两轴都非零)。
    """
    if not isinstance(raw, dict):
        return None
    wrapped = {
        "op_id": "srv:0", "site": "srv", "lam": 1,
        "ts": raw.get("ts") or 0, "base_rev": 0,
    }
    for k, v in raw.items():
        if k not in ENVELOPE_KEYS:
            wrapped[k] = v
    clean = validate_op(wrapped)
    if clean is None and wrapped.get("type") == "move":
        # 普通管线拒绝单轴移动; 撤销语义里 (dx,0) 与 (0,dy) 完全合法
        target = str(wrapped.get("id") or "")[:64]
        dx = _finite_number(wrapped.get("dx"))
        dy = _finite_number(wrapped.get("dy"))
        if target and dx is not None and dy is not None and (dx != 0 or dy != 0):
            clean = {"type": "move", "id": target, "dx": dx, "dy": dy}
    if clean is None:
        return None
    return {k: v for k, v in clean.items() if k not in ENVELOPE_KEYS}


def clean_leaves(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw[:MAX_ENTRY_LEAVES]:
        leaf = clean_leaf(item)
        if leaf is not None:
            out.append(leaf)
    return out[:MAX_ENTRY_LEAVES]


# ---------------------------------------------------------------- 磁盘读写
class UndoStore:
    """单「白板 × 用户」撤销文件的读写。"""

    def __init__(self, board_id: str, username: str):
        # 用户名受 USERNAME_RE 约束(字母数字下划线/中文), safe_id 再规约一次
        fname = safe_id(username) + ".json"
        self.dir = os.path.join(config.board_dir(board_id), "undo")
        self.path = os.path.join(self.dir, fname)

    def load(self) -> Dict[str, Any]:
        data = read_json(self.path, default=None)
        if not isinstance(data, dict) or not isinstance(data.get("undo"), list):
            return {"v": 1, "undo": [], "redo": []}
        if not isinstance(data.get("redo"), list):
            data["redo"] = []
        return data

    def save(self, data: Dict[str, Any]) -> None:
        os.makedirs(self.dir, exist_ok=True)
        write_json_atomic(self.path, data)


def _summary(data: Dict[str, Any]) -> Dict[str, Any]:
    """对外摘要: 只带 gid/label, 不含操作体(welcome/undo_state 用)。"""
    def brief(stack: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{"gid": e.get("gid"), "label": e.get("label") or ""}
                for e in stack if isinstance(e, dict)]
    return {"undo": brief(data.get("undo") or []),
            "redo": brief(data.get("redo") or [])}


def _gid_seen(data: Dict[str, Any], gid: str) -> bool:
    for stack in (data.get("undo") or [], data.get("redo") or []):
        for e in stack:
            if isinstance(e, dict) and e.get("gid") == gid:
                return True
    return False


# ---------------------------------------------------------------- 并发冲突规划
def plan_inverse(history_ops: List[Dict[str, Any]], undo_user: str,
                 leaves: List[Dict[str, Any]], truncated: bool,
                 redo: bool = False) -> List[Dict[str, Any]]:
    """根据撤销单元之后他人的操作, 过滤出「安全」的逆操作叶子。

    ``history_ops`` 为撤销单元 base_rev 之后(升序)的非 move 记录;
    ``truncated`` 表示扫描因达到上限被截断 —— 此时除天然可交换的
    move 外一律跳过(宁可不撤销, 也不覆盖未知的他人修改)。

    规则(undo 是「逆操作」, redo 是「重放原操作」, 冲突判定对称):
    * move 叶子恒保留(增量可交换, 与任何人的位移相加);
    * set_props 按字段跳过已被他人写过的字段, 全部被跳过则丢弃整条;
    * reorder/reparent/path_extend/truncate_path 同理按「字段」跳过;
    * redo=False(撤销 add → delete): 他人碰过该图形则整条丢弃,
      避免把他人的后续工作连同图形一起删掉;
    * redo=True(重放 add): 他人一旦碰过该图形即跳过添加
      (对方已删除/改造时不应凭空再建);
    * restore_shape(撤销 delete)恒保留(LWW 收敛, 复活不覆盖字段)。
    """
    touched_fields: Dict[str, Set[str]] = {}
    touched_ids: Set[str] = set()

    if not truncated:
        for rec in history_ops:
            if rec.get("by") == undo_user:
                continue                       # 自己后续的编辑不算冲突
            subs = rec.get("ops") if rec.get("type") == "batch" else [rec]
            for sub in subs if isinstance(subs, list) else [rec]:
                if not isinstance(sub, dict):
                    continue
                stype = sub.get("type")
                sid = sub.get("id") or (sub.get("shape") or {}).get("id")
                if not sid:
                    continue
                if stype == "move":
                    continue                   # 增量域不冲突
                touched_ids.add(sid)
                bucket = touched_fields.setdefault(sid, set())
                if stype == "set_props":
                    for field in (sub.get("props") or {}):
                        bucket.add(str(field))
                elif stype == "reorder":
                    bucket.add("z")
                elif stype == "reparent":
                    bucket.add("parent")
                elif stype in ("path_extend", "truncate_path"):
                    bucket.add("points")
                elif stype == "add_shape":
                    bucket.add("__add__")
                else:
                    bucket.add("deleted")

    kept: List[Dict[str, Any]] = []
    for leaf in leaves:
        ltype = leaf.get("type")
        if ltype == "move":
            kept.append(leaf)
            continue
        if truncated:
            continue
        sid = leaf.get("id") or (leaf.get("shape") or {}).get("id")
        if ltype == "delete_shape":
            # undo 时 add 的逆; redo 时「重放删除」。两者都要求:
            # 撤销窗口内他人没有再碰过该图形。
            if sid and touched_fields.get(sid):
                continue
            kept.append(leaf)
        elif ltype == "add_shape":
            # 仅出现在 redo: 他人碰过该 id(删除/重建/改造)则不重新添加
            if sid and touched_fields.get(sid):
                continue
            kept.append(leaf)
        elif ltype == "restore_shape":
            kept.append(leaf)
        elif ltype == "set_props":
            fields = touched_fields.get(sid, set())
            props = {f: v for f, v in (leaf.get("props") or {}).items()
                     if f not in fields}
            if props:
                kept.append({**leaf, "props": props})
        elif ltype == "reorder":
            if "z" not in touched_fields.get(sid, set()):
                kept.append(leaf)
        elif ltype == "reparent":
            if "parent" not in touched_fields.get(sid, set()):
                kept.append(leaf)
        elif ltype in ("truncate_path", "path_extend"):
            if "points" not in touched_fields.get(sid, set()):
                kept.append(leaf)
    return kept


# ---------------------------------------------------------------- 服务
class UndoService:
    """撤销文件的缓存、串行锁与栈操作; 文档状态变更由 BoardManager 完成。"""

    def __init__(self) -> None:
        self._locks: Dict[str, asyncio.Lock] = {}
        # req_id → (时间戳, direction, 已签发 op): 断线重连重复请求幂等
        self._req_cache: "OrderedDict[str, Tuple[int, str, Dict[str, Any]]]" = OrderedDict()

    def _key(self, board_id: str, username: str) -> str:
        return f"{board_id}\x00{username}"

    def user_lock(self, board_id: str, username: str) -> asyncio.Lock:
        key = self._key(board_id, username)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def store(self, board_id: str, username: str) -> UndoStore:
        return UndoStore(board_id, username)

    # ------------------------------------------------------------ 摘要
    def state(self, board_id: str, username: str) -> Dict[str, Any]:
        return _summary(self.store(board_id, username).load())

    # ------------------------------------------------------------ checkpoint
    def register(self, board_id: str, username: str, gid: str, label: str,
                 undo_leaves: List[Dict[str, Any]], redo_leaves: List[Dict[str, Any]],
                 base_rev: int, depth: int) -> Dict[str, Any]:
        """登记一个撤销单元(手势结束/原子提交)。返回新栈摘要。"""
        store = self.store(board_id, username)
        data = store.load()
        if _gid_seen(data, gid):
            return _summary(data)                    # 重连补发同一 checkpoint, 幂等
        entry = {
            "gid": gid,
            "label": label[:MAX_LABEL_LEN],
            "ts": now_ms(),
            "base_rev": int(base_rev or 0),
            "undo": undo_leaves,
            "redo": redo_leaves,
        }
        undo = data.setdefault("undo", [])
        undo.append(entry)
        data["redo"] = []                            # 新动作清掉重做栈
        depth = max(1, min(int(depth or 100), 500))
        if len(undo) > depth:
            del undo[:len(undo) - depth]
        self._prune_bytes(store, data)
        store.save(data)
        return _summary(data)

    def _prune_bytes(self, store: UndoStore, data: Dict[str, Any]) -> None:
        undo = data.setdefault("undo", [])
        # 单条目过大先丢弃, 仍超总上限则从最旧撤销单元开始修剪
        for e in list(undo):
            if len(_dumps(e)) > MAX_ENTRY_JSON_BYTES:
                undo.remove(e)
        while undo and len(_dumps(data)) > MAX_STATE_JSON_BYTES:
            undo.pop(0)

    # ------------------------------------------------------------ 栈弹出/压回
    def pop(self, board_id: str, username: str, direction: str
            ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """读出并从源栈弹出栈顶条目(尚未落盘, 由 commit 保存)。"""
        store = self.store(board_id, username)
        data = store.load()
        src = data[UNDO if direction == UNDO else REDO]
        if not src:
            return data, {}
        entry = src.pop()
        dst = data[REDO if direction == UNDO else UNDO]
        dst.append(entry)
        return data, entry

    @staticmethod
    def restore_popped(data: Dict[str, Any], direction: str) -> None:
        """逆操作无法执行(全部被冲突跳过)时, 把条目放回源栈。"""
        if direction == UNDO:
            src, dst = data.get("undo"), data.get("redo")
        else:
            src, dst = data.get("redo"), data.get("undo")
        if isinstance(src, list) and isinstance(dst, list) and dst:
            src.append(dst.pop())

    def find_issued(self, board_id: str, username: str, req_id: str
                    ) -> Optional[Dict[str, Any]]:
        """跨进程重启后的幂等: 在两个栈里找该 req_id 已签发过的逆操作。"""
        data = self.store(board_id, username).load()
        for stack in (data.get("undo") or [], data.get("redo") or []):
            for entry in stack:
                issued = (entry or {}).get("issued") if isinstance(entry, dict) else None
                if isinstance(issued, dict):
                    for record in issued.values():
                        if isinstance(record, dict) and record.get("req_id") == req_id:
                            return record.get("op")
        return None

    def commit(self, board_id: str, username: str, data: Dict[str, Any]) -> Dict[str, Any]:
        store = self.store(board_id, username)
        self._prune_bytes(store, data)
        store.save(data)
        return _summary(data)

    # ------------------------------------------------------------ 幂等请求
    def remember_request(self, req_id: str, direction: str,
                         op: Dict[str, Any]) -> None:
        self._req_cache[req_id] = (int(time.time()), direction, op)
        self._req_cache.move_to_end(req_id)
        while len(self._req_cache) > 4096:
            self._req_cache.popitem(last=False)
        self._gc_requests()

    def cached_request(self, req_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        hit = self._req_cache.get(req_id)
        if hit is None:
            return None
        self._req_cache.move_to_end(req_id)
        return hit[1], hit[2]

    def _gc_requests(self) -> None:
        cutoff = int(time.time()) - 600
        for key in list(self._req_cache):
            if self._req_cache[key][0] < cutoff:
                self._req_cache.pop(key, None)
            else:
                break

    def drop_board(self, board_id: str) -> None:
        prefix = board_id + "\x00"
        for key in [k for k in self._locks if k.startswith(prefix)]:
            self._locks.pop(key, None)


def _dumps(obj: Any) -> bytes:
    import json
    return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


# ---------------------------------------------------------------- 逆操作签发
def issue_inverse(doc: Any, leaves: List[Dict[str, Any]],
                  username: str) -> Optional[Dict[str, Any]]:
    """用服务端站点把一批语义叶子签发为一个(可 batch 的)新操作并应用。

    返回带 rev 的顶层操作(单叶子即叶子本身, 多叶子包 batch); 无有效
    叶子返回 None。新 op_id 带随机后缀, 与任何历史操作(含同一用户其他
    设备签发过的逆操作)都不会撞幂等表。
    """
    valid = [clean_leaf(leaf) for leaf in leaves]
    valid = [leaf for leaf in valid if leaf is not None]
    if not valid:
        return None
    ts = now_ms()
    # 服务端时钟必须高于文档见过的任何客户端时钟, 保证 LWW 逆操作生效
    lam0 = max(int(getattr(doc, "lam_witness", 0) or 0),
               int(getattr(doc, "head_rev", 0) or 0)) + 1
    subs: List[Dict[str, Any]] = []
    for i, leaf in enumerate(valid):
        sub = {
            "op_id": f"{SERVER_SITE}:{secrets.token_hex(8)}",
            "site": SERVER_SITE,
            "lam": lam0 + i,
            "ts": ts,
            "base_rev": 0,
        }
        sub.update(leaf)
        subs.append(sub)
    if len(subs) == 1:
        top = subs[0]
    else:
        top = {
            "op_id": f"{SERVER_SITE}:{secrets.token_hex(8)}",
            "site": SERVER_SITE,
            "lam": lam0 + len(subs),
            "ts": ts,
            "type": "batch",
            "base_rev": 0,
            "ops": subs,
        }
    doc.lam_witness = max(doc.lam_witness, int(top["lam"]))
    doc.apply_op(top, by=username)
    return top


undo_service = UndoService()
