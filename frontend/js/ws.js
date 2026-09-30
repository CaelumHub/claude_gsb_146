/* ================================================================
   ws.js —— 协同 WebSocket 客户端

   职责(对应「断线重连状态同步与操作补发」难点):
   - 指数退避 + 抖动的自动重连;
   - last_rev 持久化到 localStorage: 刷新/断线重连后 hello 带上,
     服务端只补发错过的操作(ring → 磁盘 → 全量快照 三级降级);
   - 有序「发件箱」outbox 同时容纳编辑操作(ops)与撤销控制消息
     (undo_checkpoint/undo/redo), 严格 FIFO 发送 —— 这保证
     「操作先于其 checkpoint」「撤销请求先于撤销后的新操作」的
     服务端处理顺序; 断线期间的条目持久化, 重连后自动补发;
     服务端按 op_id / req_id / gid 幂等去重, 不会重复生效;
   - 心跳: 应答服务端 ping, 且每 25s 主动 ping 一次, 60s 无任何
     消息则视为假死连接, 主动断开触发重连;
   - 事件分发: on(type, handler), 状态变化广播 wb:conn-status。
   ================================================================ */
import { Api } from './api.js';

const LS = {
  rev: (boardId) => `wb_rev_${boardId}`,
  outbox: (boardId) => `wb_pending_${boardId}`,
  clientId: (boardId) => `wb_client_${boardId}`,
};

function lsGet(key, fallback = null) {
  try {
    const raw = localStorage.getItem(key);
    return raw == null ? fallback : JSON.parse(raw);
  } catch { return fallback; }
}

function lsSet(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* 配额满/隐私模式 */ }
}

export function getClientId(boardId) {
  let id = lsGet(LS.clientId(boardId));
  if (!id || typeof id !== 'string') {
    id = (crypto?.randomUUID ? crypto.randomUUID().replace(/-/g, '') : Math.random().toString(36).slice(2))
      .slice(0, 16);
    lsSet(LS.clientId(boardId), id);
  }
  return id;
}

/** 旧版本地撤销栈遗留的纯操作数组 → 新版有序发件箱条目 */
function migrateOutbox(raw) {
  if (!Array.isArray(raw)) return [];
  if (raw.every((e) => e && (e.kind === 'ops' || e.kind === 'ctrl'))) return raw;
  const ops = raw.filter(Boolean);
  return ops.length ? [{ kind: 'ops', ops }] : [];
}

export class BoardSocket {
  /**
   * @param {string} boardId
   * @param {object} opts
   *   page      —— 页面标识(editor/mindmap/chat/...)
   *   clientId  —— 站点 ID(缺省自动生成并持久化)
   *   persist   —— 是否持久化 rev/队列(默认 true; 回放页等只读场景可关)
   */
  constructor(boardId, opts = {}) {
    this.boardId = boardId;
    this.page = opts.page || 'editor';
    this.persist = opts.persist !== false;
    this.clientId = opts.clientId || getClientId(boardId);
    this.ws = null;
    this.status = 'idle';            // idle | connecting | open | reconnecting | closed
    this.manualClose = false;
    this.retry = 0;
    this.lastRev = this.persist ? (lsGet(LS.rev(boardId), 0) || 0) : 0;
    // 有序发件箱: [{kind:'ops', ops:[...]}, {kind:'ctrl', msg:{...}}]
    this.outbox = this.persist ? migrateOutbox(lsGet(LS.outbox(boardId), [])) : [];
    this.inFlight = null;            // 已发送待确认的队头条目
    this._handlers = {};
    this._pingTimer = null;
    this._watchdog = null;
    this._reconnectTimer = null;
    this.onStatusChange = opts.onStatusChange || null;
    window.addEventListener('online', () => { if (this.status === 'reconnecting') this.connect(); });
  }

  /* ------------------------------------------------------------ 事件 */
  on(type, handler) {
    (this._handlers[type] = this._handlers[type] || []).push(handler);
    return this;
  }

  emit(type, data) {
    for (const handler of this._handlers[type] || []) {
      try { handler(data); } catch (err) { console.error(`[ws] handler(${type})`, err); }
    }
    for (const handler of this._handlers['*'] || []) {
      try { handler(type, data); } catch (err) { console.error('[ws] handler(*)', err); }
    }
  }

  _setStatus(status, extra = {}) {
    this.status = status;
    if (this.onStatusChange) this.onStatusChange(status, extra);
    window.dispatchEvent(new CustomEvent('wb:conn-status',
      { detail: { boardId: this.boardId, status, ...extra } }));
  }

  /* ------------------------------------------------------------ 连接 */
  connect() {
    if (this.status === 'open' || this.status === 'connecting') return;
    this.manualClose = false;
    this._setStatus(this.retry ? 'reconnecting' : 'connecting');
    const url = Api.wsUrl(this.boardId, {
      clientId: this.clientId, lastRev: this.lastRev, page: this.page,
    });
    let ws;
    try { ws = new WebSocket(url); } catch (err) {
      this._scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.retry = 0;
      this._setStatus('open');
      this._startHeartbeat();
    };
    ws.onmessage = (event) => this._onMessage(event.data);
    ws.onclose = (event) => {
      this._stopHeartbeat();
      if (this.manualClose) { this._setStatus('closed'); return; }
      // 4401 未登录 / 4403 无权限: 不做无意义重连
      if (event.code === 4401 || event.code === 4403) {
        this._setStatus('closed', { code: event.code, fatal: true });
        this.emit('fatal', { code: event.code });
        return;
      }
      this._scheduleReconnect();
    };
    ws.onerror = () => { /* onclose 会跟进处理 */ };
  }

  _scheduleReconnect() {
    if (this.manualClose) return;
    this.retry += 1;
    const base = Math.min(30_000, 800 * 2 ** Math.min(this.retry, 5));
    const delay = base * (0.7 + Math.random() * 0.6);   // 抖动防雪崩
    this._setStatus('reconnecting', { retry: this.retry, delay: Math.round(delay) });
    clearTimeout(this._reconnectTimer);
    this._reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  close() {
    this.manualClose = true;
    clearTimeout(this._reconnectTimer);
    this._stopHeartbeat();
    try { this.ws?.close(); } catch { /* 忽略 */ }
    this.ws = null;
    this._setStatus('closed');
  }

  /* ------------------------------------------------------------ 心跳 */
  _startHeartbeat() {
    this._stopHeartbeat();
    this._bumpWatchdog();
    this._pingTimer = setInterval(() => this._send({ type: 'ping' }), 25_000);
  }

  _bumpWatchdog() {
    clearTimeout(this._watchdog);
    this._watchdog = setTimeout(() => {
      // 60s 没有任何下行消息 → 假死, 主动断开走重连
      try { this.ws?.close(); } catch { /* 忽略 */ }
    }, 60_000);
  }

  _stopHeartbeat() {
    clearInterval(this._pingTimer);
    clearTimeout(this._watchdog);
  }

  /* ------------------------------------------------------------ 收发 */
  _send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
      return true;
    }
    return false;
  }

  _onMessage(raw) {
    this._bumpWatchdog();
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const type = msg.type;

    if (type === 'welcome') {
      if (msg.state) {
        this.lastRev = msg.state.rev ?? msg.head_rev ?? 0;
        this._saveRev();
      } else if (msg.catchup?.ops?.length) {
        this._applyCatchup(msg.catchup.ops);
      }
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, msg.head_rev); this._saveRev(); }
      this._flushOutbox();
      this.emit('welcome', msg);
      return;
    }
    if (type === 'ops') {
      if (msg.head_rev != null) { this.lastRev = msg.head_rev; this._saveRev(); }
      this.emit('ops', msg);
      return;
    }
    if (type === 'ack') {
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, msg.head_rev); this._saveRev(); }
      const acked = new Set((msg.acks || []).map((a) => a.op_id));
      this._resolveOpsAck(acked);
      this.emit('ack', msg);
      // 服务端单批最多接受 max_ops_per_batch 条, 剩余队列在此续排
      this._flushOutbox();
      return;
    }
    if (type === 'ctrl_ack') {
      this._resolveCtrlAck(msg);
      this._flushOutbox();
      this.emit('ctrl_ack', msg);
      return;
    }
    if (type === 'undo_state') {
      this.emit('undo_state', msg);
      return;
    }
    if (type === 'error') {
      // 队头被服务端拒绝时不要让发件箱永久卡死: 丢弃队头继续发送
      if ((msg.code === 'invalid_op' || msg.code === 'undo_failed') && this.inFlight) {
        this.inFlight = null;
        this.outbox.shift();
        this._saveOutbox();
        this._flushOutbox();
      }
    }
    if (type === 'ping') { this._send({ type: 'ping' }); return; }   // 服务端心跳 → 应答
    if (type === 'pong') return;
    this.emit(type, msg);
  }

  _applyCatchup(ops) {
    // 补发的操作交给页面合并; 这里只推进 rev
    let maxRev = this.lastRev;
    for (const op of ops || []) maxRev = Math.max(maxRev, op.rev || 0);
    this.lastRev = maxRev;
    this._saveRev();
    this.emit('catchup', { ops: ops || [] });
  }

  _saveRev() {
    if (this.persist) lsSet(LS.rev(this.boardId), this.lastRev);
  }

  /* ------------------------------------------------------------ 发件箱 */
  /** 发送一个(或一批)本地 CRDT 操作; 断线时进入持久化发件箱补发。 */
  sendOps(ops) {
    const list = Array.isArray(ops) ? ops : [ops];
    if (!list.length) return;
    // 单条 WS 消息最多 64 个操作, 超出切片(保持 FIFO)
    for (let i = 0; i < list.length; i += 64) {
      this._enqueue({ kind: 'ops', ops: list.slice(i, i + 64) });
    }
    this._flushOutbox();
  }

  sendOp(op) { this.sendOps([op]); }

  /** 入队一条撤销控制消息(checkpoint/undo/redo), 排在操作之后保序 */
  sendControl(msg) {
    if (!msg || typeof msg !== 'object') return;
    this._enqueue({ kind: 'ctrl', msg });
    this._flushOutbox();
  }

  _enqueue(entry) {
    this.outbox.push(entry);
    if (this.outbox.length > 500) this.outbox = this.outbox.slice(-500);
    this._saveOutbox();
  }

  /** 严格 FIFO: 一次只发队头条目, 收到确认后再发下一条 */
  _flushOutbox() {
    if (this.status !== 'open' || this.inFlight || !this.outbox.length) return;
    const entry = this.outbox[0];
    const payload = entry.kind === 'ops'
      ? { type: 'ops', ops: entry.ops }
      : entry.msg;
    if (this._send(payload)) {
      this.inFlight = entry;
      this.emit('pending-flushed', { remaining: this.outbox.length - 1 });
    }
  }

  _resolveOpsAck(ackedIds) {
    const head = this.inFlight;
    if (!head || head.kind !== 'ops' || !ackedIds.size) return;
    const rest = head.ops.filter((op) => !ackedIds.has(op.op_id));
    this.inFlight = null;
    this.outbox.shift();
    // 极少数(整条被拒)情况下保留未确认操作到队头, 下个周期重发,
    // 已确认的服务端按 op_id 幂等去重, 不会二次生效。
    if (rest.length) this.outbox.unshift({ kind: 'ops', ops: rest });
    this._saveOutbox();
  }

  _resolveCtrlAck(msg) {
    const head = this.inFlight;
    if (!head || head.kind !== 'ctrl') return;
    const want = head.msg;
    let match = false;
    if (msg.kind === 'checkpoint') match = want.type === 'undo_checkpoint' && want.gid === msg.gid;
    else match = want.type === msg.kind && want.req_id === msg.req_id;
    if (!match) return;
    this.inFlight = null;
    this.outbox.shift();
    this._saveOutbox();
  }

  _saveOutbox() {
    if (this.persist) lsSet(LS.outbox(this.boardId), this.outbox);
  }

  pendingCount() {
    return this.outbox.length + (this.inFlight ? 1 : 0);
  }

  /* ------------------------------------------------------------ 其他消息 */
  sendChat(text) { return this._send({ type: 'chat', text }); }

  sendPresence(payload) { return this._send({ type: 'presence', ...payload }); }

  sendHello(page) { return this._send({ type: 'hello', page: page || this.page, client_id: this.clientId, last_rev: this.lastRev }); }
}
