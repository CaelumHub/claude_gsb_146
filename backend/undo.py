"""服务端撤销/重做 —— 按 (白板, 用户) 持久化的语义操作栈。

解决的问题
==========
撤销历史原本只存在浏览器内存(CrdtClient.undoStack), 刷新页面或换设备
登录后无法撤销之前的操作。本模块把撤销单元记录在服务端, 与操作日志一起
落盘: 任何设备以同一账号打开同一白板, welcome 即下发栈深度与最近标签,
撤销/重做请求也由服务端在 per-board 锁内执行。

**撤销单元如何识别**
客户端给一个撤销动作包含的每个叶子操作打上相同的 ``ug`` (undo group id),
并可选带 ``ulabel``(中文标签)。接入管线(:meth:`BoardManager.ingest_ops`)
在校验通过、分配 rev 之后调用 :meth:`UndoStore.record`。同一 ug 的叶子
(可能分多条 WS 消息到达 —— 手绘/拖动正是如此)按 (id, 类型) 聚合成同一个
entry。不带 ug 的操作(服务端模板、旧客户端)不入栈。

**撤销 = 带并发保护的语义逆操作(不是回滚)**
entry 记录的叶子只有在「目标字段的最后写入者仍是用户自己的站点」时才会
生成逆操作; 被他人并发改过的字段被跳过 —— 撤销自己的操作绝不覆盖他人的
并发编辑(对应 set_props/reorder/reparent/delete 的字段级 LWW 语义)。
move 是增量 CRDT, 逆增量永远安全(可交换、累加)。

逆操作以全新的 op_id/lamport 作为**普通操作**重新进入接入管线 → 记日志、
广播、断线补发全复用既有路径, 所有副本(含其他设备上的自己)自然收敛。

落盘
====
``boards/<bid>/undo/<safe(username)>.json`` 原子写(沿用 write_json_atomic);
站点归属表 ``boards/<bid>/undo/_sites.json`` (site → username)。写盘由
接入路径标脏、心跳巡检统一 flush(与快照节流一致), undo/redo 执行后立即
flush, 崩溃最多丢失最近几秒的栈记录(文档状态本身不受影响)。
"""
from __future__ import annotations

import itertools
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from . import config
from .crdt import BoardDoc, Clock
from .storage import read_json, safe_id, write_json_atomic

#: undo 叶子记录里需要落盘的字段(其余信封字段不入栈)
MAX_ENTRY_LEAVES = 512
MAX_LABEL_LEN = 40
MAX_GID_LEN = 80
MAX_PATH_POINTS_LEAF = 20000       # 与 crdt.MAX_POINTS 对齐, 防单步手绘撑爆栈文件

#: 服务端代发 undo/redo 操作的站点前缀。站点按用户名确定性派生(而非随机),
#: 保证换设备/进程重启后「这个字段的最后写入者是不是我自己」的判定恒成立;
#: lam 取 doc.lam_witness 递增序列, 重启后只会更大, LWW 单调性不受影响。
SERVER_SITE_PREFIX = "srv-undo-"


def server_site(username: str) -> str:
    return SERVER_SITE_PREFIX + safe_id(username, maxlen=40)

# 站点→用户归属表(跨白板共用, 存 data/site_registry.json)。
# 用户在设备 A 撤销设备 B 的操作时, 凭这张表判断「这个字段的最后写入者
# 是不是我自己」, 而不是凭当前连接的 site。
_sites_lock = threading.Lock()
_sites: Dict[str, str] = {}
_sites_loaded = False
_sites_dirty = False
SITE_REGISTRY_FILE = os.path.join(config.DATA_DIR, "site_registry.json")


def _load_sites() -> None:
    global _sites_loaded
    if _sites_loaded:
        return
    data = read_json(SITE_REGISTRY_FILE, default={}) or {}
    if isinstance(data, dict):
        for site, user in list(data.items())[:100_000]:
            _sites[str(site)[:64]] = str(user)[:64]
    _sites_loaded = True


def register_site(site: str, username: str) -> bool:
    """登记 site → username; 返回是否为新登记(用于标脏落盘)。"""
    if not site or not username:
        return False
    _load_sites()
    with _sites_lock:
        if _sites.get(site) == username:
            return False
        _sites[site] = username
        global _sites_dirty
        _sites_dirty = True
        return True


def site_owner(site: Optional[str]) -> Optional[str]:
    _load_sites()
    return _sites.get(site or "")


def flush_sites_registry() -> None:
    """把站点归属表原子落盘(心跳巡检调用)。"""
    global _sites_dirty
    with _sites_lock:
        if not _sites_dirty:
            return
        write_json_atomic(SITE_REGISTRY_FILE, _sites)
        _sites_dirty = False


# ---------------------------------------------------------------- 叶子结构
# 每个 entry 的 leaves 按目标图形聚合, 形态:
#   {"k":"move",   "id":..., "dx","dy":}                    累计增量
#   {"k":"set",    "id":..., "fields":{f: {"v","before","clock":[lam,site]}}}
#   {"k":"reorder","id":..., "z","before","clock"}
#   {"k":"reparent","id":..., "parent","before","clock"}
#   {"k":"add",    "id":..., "shape":..., "clock"}          add_shape
#   {"k":"del",    "id":..., "delete":bool, "clock"}        delete/restore
#   {"k":"path",   "id":..., "length":int, "points":[...],"ext":bool}
#
# path 叶子跨「手绘的多帧续笔」聚合为一个跨度: length=本组首帧之前的长度
# (撤销时截回的位置), points=本组所有续笔点按序拼接(撤销=截回, 重做=续回)。
# truncate(截短)则记录截点与被截掉的点。


def _fc(shape: Optional[Dict[str, Any]], field: str) -> Optional[Clock]:
    return BoardDoc._fc_get(shape or {}, field)


def _clone(v: Any) -> Any:
    return json.loads(json.dumps(v, ensure_ascii=False, default=str))


def _leaf_merge(entry: Dict[str, Any], leaf: Dict[str, Any]) -> None:
    """把同一 gid 的新叶子并入 entry(按 id+类型聚合, 后到覆盖前到)。"""
    leaves = entry["leaves"]
    kind = leaf["k"]
    sid = leaf["id"]
    for existing in leaves:
        if existing["k"] == kind and existing.get("id") == sid:
            if kind == "move":
                existing["dx"] = round(float(existing.get("dx") or 0) + float(leaf.get("dx") or 0), 6)
                existing["dy"] = round(float(existing.get("dy") or 0) + float(leaf.get("dy") or 0), 6)
                return
            if kind == "set":
                fields = existing.setdefault("fields", {})
                for f, rec in leaf.get("fields", {}).items():
                    prev = fields.get(f)
                    if prev is None:
                        fields[f] = rec                      # 首个赢家: 保留 before
                    else:
                        fields[f] = {"v": rec["v"], "before": prev["before"],
                                     "clock": rec["clock"]}
                return
            if kind == "path":
                if leaf.get("ext"):
                    # 多帧续笔聚合为一个跨度: 保留首帧之前的长度, 逐帧拼点
                    existing["length"] = int(existing.get("length") or 0)
                    merged = list(existing.get("points") or [])
                    merged.extend(leaf.get("points") or [])
                    existing["points"] = merged[:MAX_PATH_POINTS_LEAF]
                else:
                    # 截短: 以后到为准(客户端只会发自己的单次截断)
                    leaves[leaves.index(existing)] = leaf
                return
            # reorder/reparent/add/del: 同 id 同类型以后到为准
            leaves[leaves.index(existing)] = leaf
            return
    leaves.append(leaf)


# ---------------------------------------------------------------- 单白板存储
class UndoStore:
    """单白板内所有用户的撤销/重做栈(惰性装载, 进程内缓存)。"""

    def __init__(self, board_id: str):
        self.board_id = board_id
        self.dir = os.path.join(config.board_dir(board_id), "undo")
        self._users: Dict[str, Dict[str, Any]] = {}
        self._dirty: set = set()

    def _depth(self) -> int:
        return max(1, int(config.get_settings().get("undo_stack_depth") or 100))

    def _path(self, username: str) -> str:
        return os.path.join(self.dir, safe_id(username, prefix="u_") + ".json")

    def _load(self, username: str) -> Dict[str, Any]:
        data = self._users.get(username)
        if data is not None:
            return data
        raw = read_json(self._path(username), default=None)
        if isinstance(raw, dict) and isinstance(raw.get("undo"), list):
            data = {"undo": raw["undo"][: self._depth()],
                    "redo": raw.get("redo") if isinstance(raw.get("redo"), list) else []}
        else:
            data = {"undo": [], "redo": []}
        self._users[username] = data
        return data

    def mark_dirty(self, username: str) -> None:
        self._dirty.add(username)

    def flush(self) -> int:
        """落盘所有标脏用户的栈, 并顺带落盘站点归属表; 返回写盘文件数。"""
        if not self._dirty:
            flush_sites_registry()
            return 0
        os.makedirs(self.dir, exist_ok=True)
        written = 0
        for username in list(self._dirty):
            data = self._users.get(username)
            if data is not None:
                write_json_atomic(self._path(username), {
                    "v": 1, "board_id": self.board_id, "user": username,
                    "undo": data["undo"], "redo": data["redo"],
                })
                written += 1
            self._dirty.discard(username)
        flush_sites_registry()
        return written

    def history(self, username: str) -> Dict[str, Any]:
        """welcome 用: 栈深度 + 最近若干条标签。"""
        data = self._load(username)
        undo = data["undo"]
        redo = data["redo"]
        return {
            "undo_depth": len(undo),
            "redo_depth": len(redo),
            "labels": [e.get("label") or "" for e in undo[-20:]],
            "last_label": (undo[-1].get("label") if undo else ""),
        }

    # ------------------------------------------------------------ 记录
    def record(self, doc: BoardDoc, op: Dict[str, Any], username: str,
               before: Dict[str, Dict[str, Any]]) -> None:
        """接入管线在校验+应用之后调用: 把一个带 ug 的已生效叶子记入栈。

        before: 该批操作应用前, 受影响图形的浅拷贝快照(id → shape)。
        只记录「真正赢了」的写入(字段时钟等于本 op 时钟), 避免把被 LWW
        判负的并发写入错误地纳入撤销单元。
        """
        gid = str(op.get("ug") or "")[:MAX_GID_LEN]
        if not gid:
            return
        label = str(op.get("ulabel") or "")[:MAX_LABEL_LEN]
        clock: Clock = (int(op.get("lam") or 0), str(op.get("site") or ""))
        leaf = self._build_leaf(doc, op, clock, before)
        if leaf is None:
            return
        data = self._load(username)
        # gid 命中已有 entry: undo 栈顶继续聚合(手势分片); 或在 redo 栈里
        # (undo 后又补发同组尾帧)并入对应 entry。否则新建一步并清空 redo。
        target = None
        if data["undo"] and data["undo"][-1].get("gid") == gid:
            target = data["undo"][-1]
        else:
            for entry in reversed(data["redo"]):
                if entry.get("gid") == gid:
                    target = entry
                    break
            if target is None:
                target = {"gid": gid, "label": label, "leaves": [],
                          "ts": int(op.get("ts") or 0), "sites": []}
                data["undo"].append(target)
                if len(data["undo"]) > self._depth():
                    data["undo"] = data["undo"][-self._depth():]
                data["redo"] = []
        if label and not target.get("label"):
            target["label"] = label
        site = clock[1]
        if site and site not in target["sites"]:
            target["sites"].append(site)
        _leaf_merge(target, leaf)
        if len(target["leaves"]) > MAX_ENTRY_LEAVES:
            target["leaves"] = target["leaves"][-MAX_ENTRY_LEAVES:]
        self.mark_dirty(username)

    def _build_leaf(self, doc: BoardDoc, op: Dict[str, Any], clock: Clock,
                    before: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """按当前(应用后)状态判断 op 赢了哪些写入, 构造落盘叶子; 全败返回 None。"""
        t = op["type"]
        sid = op.get("id") or (op.get("shape") or {}).get("id")
        if not sid:
            return None
        shape = doc.shapes.get(sid)
        pre = before.get(sid) or {}

        if t == "move":
            return {"k": "move", "id": sid,
                    "dx": float(op.get("dx") or 0.0), "dy": float(op.get("dy") or 0.0)}

        if t == "add_shape":
            # 只有「本次 add 确实让图形存在/复活」才入栈(deleted 时钟为本次)。
            # shape 取 add 操作自身的载荷快照(后续手绘续笔等不会污染重做形状)。
            if shape is None or _fc(shape, "deleted") != clock:
                return None
            shape_in = op["shape"]
            clean_shape = {k: _clone(v) for k, v in shape_in.items() if k != "fc"}
            return {"k": "add", "id": sid, "shape": clean_shape,
                    "clock": list(clock)}

        if t in ("delete_shape", "restore_shape"):
            want_delete = t == "delete_shape"
            if shape is None or _fc(shape, "deleted") != clock:
                return None
            return {"k": "del", "id": sid, "delete": want_delete,
                    "clock": list(clock)}

        if t == "set_props":
            fields: Dict[str, Any] = {}
            for f, value in (op.get("props") or {}).items():
                won_clock = _fc(shape, f)
                if won_clock != clock:
                    continue                        # 该字段被更新的 LWW 写入判负
                fields[f] = {"v": _clone(value), "before": _clone(pre.get(f)),
                             "clock": list(clock)}
            if not fields:
                return None
            return {"k": "set", "id": sid, "fields": fields}

        if t == "reorder":
            if shape is None or _fc(shape, "z") != clock:
                return None
            return {"k": "reorder", "id": sid, "z": float(op.get("z") or 0),
                    "before": float(pre.get("z") or 0.0), "clock": list(clock)}

        if t == "reparent":
            if shape is None or _fc(shape, "parent") != clock:
                return None
            return {"k": "reparent", "id": sid,
                    "parent": op.get("parent") or None,
                    "before": pre.get("parent"), "clock": list(clock)}

        if t == "path_extend":
            pts = op.get("points") or []
            pre_len = len(pre.get("points") or [])
            return {"k": "path", "id": sid, "length": pre_len,
                    "points": _clone(pts), "ext": True}

        if t == "truncate_path":
            pre_pts = pre.get("points") or []
            return {"k": "path", "id": sid, "length": int(op.get("length") or 0),
                    "points": _clone(pre_pts[int(op.get("length") or 0):]),
                    "ext": False}
        return None

    # ------------------------------------------------------------ 归属判定
    def _mine(self, username: str, clock: Optional[Clock]) -> bool:
        """字段时钟的站点是否属于该用户(含服务端代其撤销/重做的站点)。"""
        if not clock:
            return False
        owner = site_owner(clock[1])
        return owner == username

    # ------------------------------------------------------------ 执行
    def execute(self, doc: BoardDoc, username: str, direction: str,
                gid: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """在 per-board 锁内执行一步撤销/重做, 返回结果描述; 空栈返回 None。

        返回: {direction, gid, label, ops:[已带时钟/op_id 的叶子...], reason?}
        调用方负责把 ops 包成 batch 信封、apply_op、记日志与广播。
        """
        data = self._load(username)
        src_name, dst_name = ("undo", "redo") if direction == "undo" else ("redo", "undo")
        src, dst = data[src_name], data[dst_name]
        if not src:
            return None
        if gid:
            idx = next((i for i in range(len(src) - 1, -1, -1)
                        if src[i].get("gid") == gid), len(src) - 1)
        else:
            idx = len(src) - 1
        entry = src.pop(idx)
        ops, leaf_updates = self._emit(doc, username, entry, direction)
        if not ops:
            # 没有可安全回退的叶子(全被他人并发改动): 丢弃该步, 不入对侧栈
            self.mark_dirty(username)
            return {"direction": direction, "gid": entry.get("gid"),
                    "label": entry.get("label", ""), "ops": [], "applied": 0,
                    "reason": "conflict"}
        dst.append(entry)
        if len(dst) > self._depth():
            dst.pop(0)
        # 更新各叶子记录的时钟为本次逆操作时钟, 保证 redo/undo 链式可行
        for leaf, new_clock in leaf_updates:
            leaf["clock"] = list(new_clock)
        self.mark_dirty(username)
        return {"direction": direction, "gid": entry.get("gid"),
                "label": entry.get("label", ""), "ops": ops,
                "applied": len(ops)}

    def _emit(self, doc: BoardDoc, username: str, entry: Dict[str, Any],
              direction: str) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], Clock]]]:
        """构造受并发保护的逆操作(direction=undo)或重放操作(redo)。

        叶子直接携带 (op_id, site, lam, ts) 信封, 供调用方包 batch;
        服务站点按用户名确定性派生, 并登记到站点归属表。
        """
        register_site(server_site(username), username)
        site = server_site(username)
        out: List[Dict[str, Any]] = []
        updates: List[Tuple[Dict[str, Any], Clock]] = []
        seq = itertools.count(1)

        def fresh() -> Tuple[Dict[str, Any], Clock]:
            lam = doc.lam_witness + next(seq)
            clock: Clock = (lam, site)
            return {"op_id": f"{site}:{lam}", "site": site, "lam": lam,
                    "ts": int(time.time() * 1000)}, clock

        for leaf in entry.get("leaves", []):
            kind, sid = leaf["k"], leaf["id"]
            shape = doc.shapes.get(sid)

            if kind == "move":
                # move 是增量 CRDT: 逆增量永远安全, 不看字段时钟
                head, _ = fresh()
                head.update({"type": "move", "id": sid,
                             "dx": (-1 if direction == "undo" else 1) * float(leaf.get("dx") or 0),
                             "dy": (-1 if direction == "undo" else 1) * float(leaf.get("dy") or 0)})
                out.append(head)
                continue

            if kind == "set":
                props: Dict[str, Any] = {}
                for f, rec in leaf.get("fields", {}).items():
                    if not self._mine(username, _fc(shape, f)):
                        continue                    # 字段被他人并发改写 → 跳过
                    props[f] = rec.get("before") if direction == "undo" else rec.get("v")
                if props:
                    head, new_clock = fresh()
                    head.update({"type": "set_props", "id": sid, "props": props})
                    out.append(head)
                    for f in props:
                        leaf["fields"][f]["clock"] = list(new_clock)
                continue

            if kind == "reorder":
                if not self._mine(username, _fc(shape, "z")):
                    continue
                head, new_clock = fresh()
                head.update({"type": "reorder", "id": sid,
                             "z": float(leaf["before"]) if direction == "undo"
                                  else float(leaf.get("z") or 0)})
                out.append(head)
                updates.append((leaf, new_clock))
                continue

            if kind == "reparent":
                if not self._mine(username, _fc(shape, "parent")):
                    continue
                head, new_clock = fresh()
                head.update({"type": "reparent", "id": sid,
                             "parent": (leaf.get("before") if direction == "undo"
                                        else leaf.get("parent")) or None})
                out.append(head)
                updates.append((leaf, new_clock))
                continue

            if kind == "del":
                if not self._mine(username, _fc(shape, "deleted")):
                    continue
                head, new_clock = fresh()
                # 原操作 delete ↔ 逆操作 restore
                was_delete = bool(leaf.get("delete"))
                head["type"] = "delete_shape" if (direction == "redo") == was_delete \
                    else "restore_shape"
                head["id"] = sid
                out.append(head)
                updates.append((leaf, new_clock))
                continue

            if kind == "add":
                if direction == "undo":
                    # 删自己加的图形: 删除寄存器仍归我, 且没有他人改过该图形
                    if not self._mine(username, _fc(shape, "deleted")):
                        continue
                    if shape is not None and self._others_touched(username, shape):
                        continue
                    head, new_clock = fresh()
                    head.update({"type": "delete_shape", "id": sid})
                else:
                    # 重放 add: 目标已是被他人占用的实体(非占位), 或被他人
                    # 删除占用, 则放弃; 仅对空占位/本人墓碑执行复活式重放。
                    if shape is not None and shape.get("kind") != "ghost":
                        if self._others_touched(username, shape) or \
                                (shape.get("deleted") and
                                 not self._mine(username, _fc(shape, "deleted"))):
                            continue
                    head, new_clock = fresh()
                    head.update({"type": "add_shape", "shape": _clone(leaf["shape"])})
                out.append(head)
                updates.append((leaf, new_clock))
                continue

            if kind == "path":
                cur_len = len((shape or {}).get("points") or [])
                was_ext = bool(leaf.get("ext"))
                undoing_ext = (direction == "undo" and was_ext) or \
                              (direction == "redo" and not was_ext)
                stored_len = len(leaf.get("points") or [])
                base_len = int(leaf.get("length") or 0)
                if undoing_ext:
                    # 续笔的逆操作是截回: 尾部点数必须与本组续笔完全一致,
                    # 否则说明有他人追加(或被其他操作改动) → 跳过
                    if cur_len != base_len + stored_len:
                        continue
                    head, _ = fresh()
                    head.update({"type": "truncate_path", "id": sid, "length": base_len})
                else:
                    # 截短的逆操作是续回: 当前长度必须恰好停在截点
                    if cur_len != base_len:
                        continue
                    head, _ = fresh()
                    head.update({"type": "path_extend", "id": sid,
                                 "points": _clone(leaf.get("points") or [])})
                out.append(head)
                continue
        return out, updates

    def _others_touched(self, username: str, shape: Dict[str, Any]) -> bool:
        """图形上是否存在「非本人」写入的字段时钟(add 的并发保护)。"""
        fc = shape.get("fc") or {}
        for entry in fc.values():
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                owner = site_owner(str(entry[1]))
                if owner is not None and owner != username:
                    return True
        return False


class UndoService:
    """按 board_id 缓存 UndoStore。"""

    def __init__(self) -> None:
        self._cache: Dict[str, UndoStore] = {}

    def for_board(self, board_id: str) -> UndoStore:
        store = self._cache.get(board_id)
        if store is None:
            store = UndoStore(board_id)
            self._cache[board_id] = store
        return store

    def drop(self, board_id: str) -> None:
        store = self._cache.pop(board_id, None)
        if store is not None:
            store.flush()

    def flush_all(self) -> int:
        written = 0
        for store in self._cache.values():
            written += store.flush()
        flush_sites_registry()
        return written


undo_service = UndoService()
