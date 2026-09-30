/* ================================================================
   crdt-client.js —— 客户端 CRDT 镜像 + 操作工厂 + 撤销/重做

   与 backend/crdt.py 语义一一对应:
   - move 是增量(delta): 并发移动同一图形时两端增量相加, 收敛一致;
   - set_props / reorder / reparent / delete / restore 是字段级
     LWW 寄存器, 时钟 = (lamport, site), 平票按 site 字典序;
   - op_id 全局唯一, 服务端幂等去重, 断线补发安全。

   撤销 = 发布语义逆操作(而非回滚), 因此他人并发编辑不受影响;
   重做 = 用新时钟重新发布原变更。
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
   CrdtClient —— 操作工厂(lamport 时钟) + 服务端撤销/重做

   撤销栈的「真源」在服务端(按 白板 × 用户 持久化):
   - commit / 手势结束 → 发送 undo_checkpoint 登记一个撤销单元;
   - undo()/redo() → 发送 undo/redo 请求, 服务端签发逆操作后广播,
     本地状态以服务端操作为准(刷新、重连、换设备后语义一致);
   - 服务端通过 welcome.undo_state 与 undo_state 消息推送栈摘要,
     本类只镜像 {gid,label} 列表用于按钮可用性。
   ================================================================ */
export class CrdtClient {
  /**
   * @param {string} siteId 站点 ID(与 WS clientId 一致)
   * @param {object} [opts]
   *   onOps: (ops[]) => void  由页面注入(发送到 WS)
   *   onControl: (msg) => void  撤销/重做/checkpoint 控制消息
   */
  constructor(siteId, opts = {}) {
    this.site = siteId;
    this.lam = 0;
    this.seq = 0;
    this.undoStack = [];        // 服务端镜像: [{gid,label}]
    this.redoStack = [];
    this.maxDepth = 100;
    this.onOps = opts.onOps || null;          // (ops[]) => void  由页面注入
    this.onControl = opts.onControl || null;  // (msg) => void
    this.getBaseRev = opts.getBaseRev || (() => 0);  // 当前已知服务端 head rev
    this._seenIds = new Set();   // 已应用 op_id 去重(move 重放保护)
  }

  /** 观察到远端操作时推进 lamport 时钟 */
  witness(op) {
    const lam = Number(op?.lam) || 0;
    if (lam > this.lam) this.lam = lam;
    if (op?.type === 'batch') (op.ops || []).forEach((sub) => this.witness(sub));
  }

  /** 合并服务端/远端操作, 按 op_id 幂等去重, 返回受影响图形集合 */
  receiveOps(shapes, ops) {
    for (const op of ops || []) this.witness(op);
    const deduped = [];
    for (const op of ops || []) {
      if (op?.op_id) {
        if (this._seenIds.has(op.op_id)) continue;
        this._seenIds.add(op.op_id);
        if (this._seenIds.size > 8192) {
          this._seenIds = new Set([...this._seenIds].slice(-4096));
        }
      }
      deduped.push(op);
    }
    return mergeOps(shapes, deduped);
  }

  _next() {
    this.lam += 1;
    this.seq += 1;
    return { site: this.site, lam: this.lam, op_id: `${this.site}:${this.seq}` };
  }

  /** 控制消息用的唯一编号(与编辑 op 的 seq 独立计数) */
  _ctlId(prefix) {
    this.seq += 1;
    const rand = (crypto?.randomUUID ? crypto.randomUUID().replace(/-/g, '')
      : Math.random().toString(36).slice(2) + Date.now().toString(36)).slice(0, 24);
    return `${prefix}_${this.site}:${this.seq}:${rand}`;
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

  /* ------------------------------------------------------------ 提交与撤销 */
  /**
   * 提交一组本地操作: 乐观应用到 shapes, 发送出去, 并把撤销单元登记
   * 到服务端撤销栈(逆操作叶子 = invertOps, 重做叶子 = 原语义操作)。
   * @param {Map} shapes
   * @param {object[]} ops 叶子操作数组(commit 内部负责包 batch)
   */
  commit(shapes, ops, { label = '' } = {}) {
    const list = (Array.isArray(ops) ? ops : [ops]).filter(Boolean);
    if (!list.length) return null;
    const captured = snapshotAffected(shapes, list);
    mergeOps(shapes, list);
    list.forEach((op) => op.op_id && this._seenIds.add(op.op_id));
    const inverse = invertOps(list, captured);           // 语义逆操作
    const recipe = list.map(cloneOp);                    // 语义原操作(重做用)
    const gid = this._ctlId('g');
    const entry = { gid, label };
    this.undoStack.push(entry);
    if (this.undoStack.length > this.maxDepth) this.undoStack.shift();
    this.redoStack.length = 0;
    this._emit(list);
    // 撤销冲突扫描窗口的下界 = 提交时刻的服务端 head rev。
    // 注意: 此刻该 head 尚未包含 list 本身, 服务端把 list 之后的操作
    // 视为「后续操作」, 自己刚发的这些叶子就是原动作, 不会误判为冲突。
    this._checkpoint(gid, label, inverse, recipe, this.getBaseRev());
    return list;
  }

  /** 只发送不记撤销(如实时拖动/缩放/手绘的中间增量帧) */
  send(shapes, ops) {
    const list = (Array.isArray(ops) ? ops : [ops]).filter(Boolean);
    if (!list.length) return;
    mergeOps(shapes, list);
    list.forEach((op) => op.op_id && this._seenIds.add(op.op_id));
    this._emit(list);
  }

  /**
   * 为「流式发送、手势结束才成一个撤销单元」的交互(拖动/缩放/手绘/擦除)
   * 登记撤销单元。undoLeaves/redoLeaves 为无信封语义叶子。
   */
  registerUndo(label, undoLeaves, redoLeaves, baseRev = 0) {
    const gid = this._ctlId('g');
    this.undoStack.push({ gid, label });
    if (this.undoStack.length > this.maxDepth) this.undoStack.shift();
    this.redoStack.length = 0;
    this._checkpoint(gid, label, undoLeaves, redoLeaves, baseRev);
    return gid;
  }

  _checkpoint(gid, label, undoLeaves, redoLeaves, baseRev = 0) {
    if (!this.onControl) return;
    this.onControl({
      type: 'undo_checkpoint', gid, label,
      undo: (undoLeaves || []).map(stripEnvelope),
      redo: (redoLeaves || []).map(stripEnvelope),
      base_rev: baseRev || 0,
    });
  }

  _emit(list) {
    if (!this.onOps) return;
    if (list.length === 1) this.onOps(list);
    else this.onOps([{ ...this._next(), type: 'batch', ts: Date.now(), base_rev: 0, ops: list }]);
  }

  canUndo() { return this.undoStack.length > 0; }
  canRedo() { return this.redoStack.length > 0; }

  /** 撤销: 请服务端签发语义逆操作(结果以 undo_state / ops 广播为准) */
  undo() {
    if (!this.canUndo()) return null;
    const entry = this.undoStack[this.undoStack.length - 1];
    if (this.onControl) {
      this.onControl({ type: 'undo', req_id: this._ctlId('u') });
    }
    return entry;
  }

  /** 重做: 请服务端用新时钟重新发布原变更 */
  redo() {
    if (!this.canRedo()) return null;
    const entry = this.redoStack[this.redoStack.length - 1];
    if (this.onControl) {
      this.onControl({ type: 'redo', req_id: this._ctlId('r') });
    }
    return entry;
  }

  /** 安装服务端推送的栈摘要(welcome / undo_state / 跨设备同步) */
  installUndoState(state) {
    const norm = (arr) => (Array.isArray(arr) ? arr
      .filter((e) => e && e.gid).map((e) => ({ gid: String(e.gid), label: e.label || '' })) : []);
    this.undoStack = norm(state?.undo);
    this.redoStack = norm(state?.redo);
  }

  clearHistory() {
    this.undoStack.length = 0;
    this.redoStack.length = 0;
  }
}

/** 剥掉操作信封, 只留语义负载(服务端 checkpoint 存储要求) */
function stripEnvelope(op) {
  if (!op || typeof op !== 'object') return op;
  if (op.type === 'batch') return { ...op, ops: (op.ops || []).map(stripEnvelope) };
  const out = {};
  for (const [k, v] of Object.entries(op)) {
    if (!['op_id', 'site', 'lam', 'ts', 'base_rev', 'rev', 'by', 'dup'].includes(k)) out[k] = v;
  }
  return out;
}

function cloneOp(op) { return JSON.parse(JSON.stringify(op)); }

/** 收集操作涉及的图形当前浅拷贝(生成逆操作用) */
export function snapshotAffected(shapes, ops) {
  const out = new Map();
  const collect = (op) => {
    if (op.type === 'batch') { (op.ops || []).forEach(collect); return; }
    const id = op.id || op.shape?.id;
    if (id && shapes.has(id) && !out.has(id)) out.set(id, cloneOp(shapes.get(id)));
  };
  (ops || []).forEach(collect);
  return out;
}

/** 构造逆操作序列(与 backend/crdt.invert_ops 语义一致) */
export function invertOps(ops, beforeMap) {
  const out = [];
  for (const op of [...(ops || [])].reverse()) {
    if (op.type === 'batch') { out.push(...invertOps(op.ops, beforeMap)); continue; }
    switch (op.type) {
      case 'add_shape':
        if (op.shape?.id) out.push({ type: 'delete_shape', id: op.shape.id });
        break;
      case 'delete_shape':
        out.push({ type: 'restore_shape', id: op.id });
        break;
      case 'restore_shape':
        out.push({ type: 'delete_shape', id: op.id });
        break;
      case 'move':
        out.push({ type: 'move', id: op.id, dx: -(op.dx || 0), dy: -(op.dy || 0) });
        break;
      case 'set_props': {
        const before = beforeMap.get(op.id) || {};
        const props = {};
        for (const field of Object.keys(op.props || {})) props[field] = before[field] ?? null;
        out.push({ type: 'set_props', id: op.id, props });
        break;
      }
      case 'reorder': {
        const before = beforeMap.get(op.id) || {};
        out.push({ type: 'reorder', id: op.id, z: before.z ?? 1 });
        break;
      }
      case 'reparent': {
        const before = beforeMap.get(op.id) || {};
        out.push({ type: 'reparent', id: op.id, parent: before.parent ?? null });
        break;
      }
      case 'path_extend': {
        const before = beforeMap.get(op.id) || {};
        out.push({ type: 'truncate_path', id: op.id, length: (before.points || []).length });
        break;
      }
      case 'truncate_path': {
        const before = beforeMap.get(op.id) || {};
        const full = (before.points || []);
        out.push({ type: 'path_extend', id: op.id, points: full.slice(op.length || 0) });
        break;
      }
      default: break;
    }
  }
  return out;
}
