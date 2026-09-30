/* ================================================================
   crdt-client.js —— 客户端 CRDT 镜像 + 操作工厂 + 撤销分组

   与 backend/crdt.py 语义一一对应:
   - move 是增量(delta): 并发移动同一图形时两端增量相加, 收敛一致;
   - set_props / reorder / reparent / delete / restore 是字段级
     LWW 寄存器, 时钟 = (lamport, site), 平票按 site 字典序;
   - op_id 全局唯一, 服务端幂等去重, 断线补发安全。

   撤销/重做是**服务端权威**的(见 backend/undo.py):
   - 每个撤销动作的叶子操作带相同的 ug(undo group id); 服务端按
     (白板, 用户)记录带 before 值与赢家时钟的撤销栈并落盘;
   - 刷新页面、断线重连、换设备登录后, welcome 带回栈深度/标签;
   - requestUndo/requestRedo 让服务端在锁内生成「带并发保护的逆操作」
     —— 被他人并发改过的字段会被跳过, 绝不覆盖协作者的编辑。
   ================================================================ */

export const LWW_FIELDS = new Set([
  'fill', 'stroke', 'strokeWidth', 'fontSize', 'fontFamily', 'fontWeight',
  'fontStyle', 'opacity', 'dash', 'text', 'textColor', 'cornerRadius',
  'collapsed', 'from', 'to', 'waypoints', 'w', 'h', 'x', 'y', 'name',
  'tag', 'strokeStyle', 'arrowStart', 'arrowEnd',
]);

export function clockOf(op) { return [Number(op.lam) || 0, String(op.site || '')]; }

/** 量化到 6 位小数(与后端 _finite_number 一致): 保证增量求和顺序无关 */
export function q6(v) { return Math.round(Number(v) * 1e6) / 1e6; }

const NUMERIC_SHAPE_FIELDS = ['x', 'y', 'w', 'h', 'z', 'rotation', 'strokeWidth',
  'fontSize', 'opacity', 'cornerRadius'];

/** 发出 add_shape 前量化图形数值(与服务端 sanitize 后的广播值一致) */
export function quantizeShape(shape) {
  const out = { ...shape };
  for (const f of NUMERIC_SHAPE_FIELDS) {
    if (out[f] != null) out[f] = q6(out[f]);
  }
  if (Array.isArray(out.points)) out.points = out.points.map((p) => [q6(p[0]), q6(p[1])]);
  return out;
}

/** 未知 id 的延迟物化占位(与后端 _placeholder 一致) */
function placeholder(shapes, id) {
  const ph = { id, kind: 'ghost', deleted: true, fc: {}, x: 0, y: 0, z: 1 };
  shapes.set(id, ph);
  return ph;
}

/** a 时钟是否严格大于 b([lam, site] 或 null) */
export function clockGt(a, b) {
  if (!a) return false;
  if (!b) return true;
  if (a[0] !== b[0]) return a[0] > b[0];
  return String(a[1]) > String(b[1]);
}

function fcGet(shape, field) {
  const entry = shape.fc?.[field];
  return Array.isArray(entry) && entry.length === 2 ? [entry[0], String(entry[1])] : null;
}

function fcSet(shape, field, clock) {
  (shape.fc = shape.fc || {})[field] = [clock[0], clock[1]];
}

function lwwPut(shape, field, value, clock) {
  if (!clockGt(clock, fcGet(shape, field))) return false;
  shape[field] = value;
  fcSet(shape, field, clock);
  return true;
}

/**
 * 把一个(远端或本地回声)操作合并进 shapes Map。幂等 + 乱序安全。
 * @param {Map<string,object>} shapes id → shape
 * @param {object} op 已带信封的操作(batch 自动展开)
 * @returns {Set<string>|null} 受影响图形 id 集合; null=无变化
 */
export function mergeOp(shapes, op) {
  if (!op || !op.type) return null;
  if (op.type === 'batch') {
    const touched = new Set();
    for (const sub of op.ops || []) {
      const subTouched = mergeOp(shapes, sub);
      if (subTouched) subTouched.forEach((id) => touched.add(id));
    }
    return touched.size ? touched : null;
  }
  const clock = clockOf(op);
  switch (op.type) {
    case 'add_shape': {
      const incoming = op.shape;
      if (!incoming?.id) return null;
      const existing = shapes.get(incoming.id);
      if (!existing) {
        const shape = { ...incoming, deleted: false, fc: { ...(incoming.fc || {}) } };
        for (const key of Object.keys(shape)) {
          if (['id', 'fc', 'meta', 'deleted'].includes(key)) continue;
          fcSet(shape, key, clock);
        }
        fcSet(shape, 'deleted', clock);
        shapes.set(shape.id, shape);
        return new Set([shape.id]);
      }
      let changed = false;
      // kind/meta 走 LWW: 墓碑(无 kind 时钟)被最大时钟的 add 吸收, 与顺序无关
      if (incoming.kind && clockGt(clock, fcGet(existing, 'kind'))) {
        existing.kind = incoming.kind;
        fcSet(existing, 'kind', clock);
        if (incoming.meta) existing.meta = incoming.meta;
        changed = true;
      }
      for (const [key, value] of Object.entries(incoming)) {
        if (['id', 'fc', 'meta', 'kind'].includes(key)) continue;
        if (key === 'x' || key === 'y') {
          // 位置基座注入(与后端一致): 未物化时把 add 的基座坐标加到
          // 已累计的 move 增量上; 已物化则跳过(re-add 不重置位置)
          if (!fcGet(existing, key)) {
            existing[key] = q6((Number(existing[key]) || 0) + (Number(value) || 0));
            fcSet(existing, key, clock);
            changed = true;
          }
          continue;
        }
        if (lwwPut(existing, key, value, clock)) changed = true;
      }
      // 复活判定统一走 deleted 字段 LWW(add 携带 deleted=False)
      if (lwwPut(existing, 'deleted', false, clock)) changed = true;
      return changed ? new Set([existing.id]) : null;
    }
    case 'move': {
      // 延迟物化 + 对已删除图形同样累计(交换律)
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      shape.x = q6((Number(shape.x) || 0) + (Number(op.dx) || 0));
      shape.y = q6((Number(shape.y) || 0) + (Number(op.dy) || 0));
      return new Set([op.id]);
    }
    case 'path_extend': {
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      (shape.points = shape.points || []).push(...(op.points || []));
      return new Set([op.id]);
    }
    case 'truncate_path': {
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      const pts = shape.points || [];
      const len = Math.max(0, Number(op.length) || 0);
      if (pts.length <= len) return null;
      shape.points = pts.slice(0, len);
      return new Set([op.id]);
    }
    case 'delete_shape':
    case 'restore_shape': {
      const wantDelete = op.type === 'delete_shape';
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      if (lwwPut(shape, 'deleted', wantDelete, clock)) return new Set([op.id]);
      return null;
    }
    case 'reorder': {
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      if (lwwPut(shape, 'z', Number(op.z) || 0, clock)) return new Set([op.id]);
      return null;
    }
    case 'reparent': {
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      const parent = op.parent || null;
      if (parent === op.id) return null;
      if (parent && createsCycle(shapes, op.id, parent)) return null;
      if (lwwPut(shape, 'parent', parent, clock)) return new Set([op.id]);
      return null;
    }
    case 'set_props': {
      const shape = shapes.get(op.id) || placeholder(shapes, op.id);
      let changed = false;
      for (const [field, value] of Object.entries(op.props || {})) {
        if (field === 'parent') continue;
        if (field === 'z' && value != null) {
          if (lwwPut(shape, 'z', Number(value) || 0, clock)) changed = true;
          continue;
        }
        if (lwwPut(shape, field, value, clock)) changed = true;
      }
      return changed ? new Set([op.id]) : null;
    }
    default:
      return null;
  }
}

export function createsCycle(shapes, nodeId, newParent) {
  let cursor = newParent;
  let hops = 0;
  while (cursor && hops < 1000) {
    if (cursor === nodeId) return true;
    cursor = shapes.get(cursor)?.parent || null;
    hops += 1;
  }
  return false;
}

/** 批量合并, 返回受影响 id 集合 */
export function mergeOps(shapes, ops) {
  const touched = new Set();
  for (const op of ops || []) {
    const ids = mergeOp(shapes, op);
    if (ids) ids.forEach((id) => touched.add(id));
  }
  return touched;
}

/** 从快照/状态接口载入全量图形 */
export function loadShapes(shapesArray) {
  const map = new Map();
  for (const shape of shapesArray || []) {
    if (shape && shape.id) map.set(shape.id, { ...shape });
  }
  return map;
}

/* ================================================================
   CrdtClient —— 操作工厂(lamport 时钟) + 服务端撤销分组
   ================================================================ */

/** 生成撤销组 id(白板内、用户内唯一即可, 服务端按用户分桶) */
export function newUndoId() {
  const rand = crypto?.randomUUID
    ? crypto.randomUUID().replace(/-/g, '').slice(0, 12)
    : Math.random().toString(36).slice(2, 14);
  return `u${Date.now().toString(36)}${rand}`;
}

/** 给一批已签发的操作打上撤销组标记(就地修改叶子) */
function stampGroup(ops, gid, label) {
  if (!gid) return ops;
  for (const op of ops || []) {
    if (!op) continue;
    op.ug = gid;
    if (label) op.ulabel = label;
    if (op.type === 'batch') stampGroup(op.ops || [], gid, label);
  }
  return ops;
}

export class CrdtClient {
  /** @param {string} siteId 站点 ID(与 WS clientId 一致) */
  constructor(siteId) {
    this.site = siteId;
    this.lam = 0;
    this.seq = 0;
    // 服务端权威的栈深度, welcome/undo_result/undo_push 时更新
    this.undoDepth = 0;
    this.redoDepth = 0;
    this.undoLabels = [];
    this.onOps = null;          // (ops[]) => void  由页面注入(发送到 WS)
    this.onHistory = null;      // ({undoDepth, redoDepth, labels}) => void
    this.onUndoRequest = null;  // (direction, {gid}) => bool 由页面注入(WS 发送)
  }

  /** 观察到远端操作时推进 lamport 时钟 */
  witness(op) {
    const lam = Number(op?.lam) || 0;
    if (lam > this.lam) this.lam = lam;
    if (op?.type === 'batch') (op.ops || []).forEach((sub) => this.witness(sub));
  }

  /** 服务端撤销栈状态(welcome.undo / undo_result / undo_push) */
  setServerHistory(h) {
    if (!h) return;
    this.undoDepth = Number(h.undo_depth) || 0;
    this.redoDepth = Number(h.redo_depth) || 0;
    if (Array.isArray(h.labels)) this.undoLabels = h.labels;
    else if (typeof h.last_label === 'string') this.undoLabels = this.undoDepth ? [h.last_label] : [];
    this.onHistory?.({ undoDepth: this.undoDepth, redoDepth: this.redoDepth,
                       labels: this.undoLabels });
  }

  _next() {
    this.lam += 1;
    this.seq += 1;
    return { site: this.site, lam: this.lam, op_id: `${this.site}:${this.seq}` };
  }

  _envelope(type, payload, baseRev = 0) {
    const head = this._next();
    return { ...head, type, ts: Date.now(), base_rev: baseRev, ...payload };
  }

  /* ------------------------------------------------------------ 操作构造
     所有数值在构造时量化到 6 位小数 —— 与服务端 sanitize 后的广播值
     一致, 保证「发送方乐观状态」与「其他副本状态」逐位相同。 */
  addShape(shape, baseRev = 0) {
    return this._envelope('add_shape', { shape: quantizeShape(shape) }, baseRev);
  }

  move(id, dx, dy, baseRev = 0) {
    return this._envelope('move', { id, dx: q6(dx), dy: q6(dy) }, baseRev);
  }

  setProps(id, props, baseRev = 0) {
    const clean = {};
    for (const [k, v] of Object.entries(props || {})) {
      clean[k] = (v != null && NUMERIC_SHAPE_FIELDS.includes(k)) ? q6(v) : v;
    }
    return this._envelope('set_props', { id, props: clean }, baseRev);
  }

  deleteShape(id, baseRev = 0) {
    return this._envelope('delete_shape', { id }, baseRev);
  }

  restoreShape(id, baseRev = 0) {
    return this._envelope('restore_shape', { id }, baseRev);
  }

  reorder(id, z, baseRev = 0) {
    return this._envelope('reorder', { id, z: q6(z) }, baseRev);
  }

  reparent(id, parent, baseRev = 0) {
    return this._envelope('reparent', { id, parent: parent || null }, baseRev);
  }

  pathExtend(id, points, baseRev = 0) {
    return this._envelope('path_extend',
      { id, points: (points || []).map((p) => [q6(p[0]), q6(p[1])]) }, baseRev);
  }

  truncatePath(id, length, baseRev = 0) {
    return this._envelope('truncate_path', { id, length }, baseRev);
  }

  batch(ops, baseRev = 0) {
    if (!ops?.length) return null;
    if (ops.length === 1) { ops[0].base_rev = baseRev; return ops[0]; }
    return this._envelope('batch', { ops }, baseRev);
  }

  /* ------------------------------------------------------------ 提交与发送
     gid = 撤销组 id。一个手势/一次命令内分片发出的多个操作(拖动的节流
     move、手绘的 path_extend、pointerup 的补发帧)复用同一个 gid, 服务端
     聚合成「一步」撤销; 普通一次性命令(commit)每次自动生成新 gid。 */
  beginGroup(label = '') {
    return { gid: newUndoId(), label };
  }

  /**
   * 提交一组本地操作: 乐观应用到 shapes, 打上撤销组标记后发送。
   * @param {Map} shapes
   * @param {object[]} ops 叶子操作数组(commit 内部负责包 batch)
   */
  commit(shapes, ops, { label = '', gid = null } = {}) {
    const list = (Array.isArray(ops) ? ops : [ops]).filter(Boolean);
    if (!list.length) return null;
    mergeOps(shapes, list);
    stampGroup(list, gid || newUndoId(), label);
    this._emit(list);
    return list;
  }

  /** 只发送不入撤销语义的便捷封装: 仍可携带 gid(手势分片), 无 gid 则不落栈 */
  send(shapes, ops, { label = '', gid = null } = {}) {
    const list = (Array.isArray(ops) ? ops : [ops]).filter(Boolean);
    if (!list.length) return;
    mergeOps(shapes, list);
    if (gid) stampGroup(list, gid, label);
    this._emit(list);
  }

  _emit(list) {
    if (!this.onOps) return;
    if (list.length === 1) { this.onOps(list); return; }
    const head = { ...this._next(), type: 'batch', ts: Date.now(), base_rev: 0, ops: list };
    // 外层 batch 继承叶子的撤销组(服务端也会兜底传播, 这里显式带上便于单测/审计)
    const g = list.find((o) => o && o.ug);
    if (g?.ug) {
      head.ug = g.ug;
      if (g.ulabel) head.ulabel = g.ulabel;
    }
    this.onOps([head]);
  }

  canUndo() { return this.undoDepth > 0; }
  canRedo() { return this.redoDepth > 0; }

  /**
   * 请求服务端撤销/重做。服务端在 per-board 锁内生成带并发保护的逆操作,
   * 经普通 ops 广播回所有副本(本页不做乐观应用, 以广播为准)。
   * @returns {boolean} 请求是否已发出(离线时 false, 调用方可提示)
   */
  requestUndo() { return this._requestHistory('undo'); }
  requestRedo() { return this._requestHistory('redo'); }

  _requestHistory(direction) {
    if (!this.onUndoRequest) return false;
    return !!this.onUndoRequest(direction, {});
  }
}

/* ------------------------------------------------------------ 兼容导出
   旧代码(tools.js 手势、回放等)曾直接使用 invertOps/snapshotAffected
   构造本地逆操作; 撤销迁移到服务端后这些仅在测试/第三方页面中可能引用,
   保留纯函数实现不影响运行。 */
