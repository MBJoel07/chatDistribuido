// Conexión tolerante a fallos con el clúster.
//
// - Prueba los servidores en orden rotativo.
// - Si un seguidor responde "redirect", se conecta directo al líder indicado.
// - Si la conexión se cae (p. ej. murió el líder), reintenta con backoff
//   creciente hasta que haya un nuevo líder elegido.

export class ChatConnection {
  constructor(servers, { onMessage, onState }) {
    this.servers = servers;
    this.onMessage = onMessage;
    this.onState = onState;
    this.ws = null;
    this.next = 0;      // próximo servidor a probar
    this.attempt = 0;   // intentos fallidos seguidos (para el backoff)
    this.nodeId = null;
  }

  start() {
    this._open(this.servers[this.next]);
  }

  send(payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }

  _open(url) {
    this.onState({ state: "connecting", url });
    let ws;
    try {
      ws = new WebSocket(url);
    } catch {
      this._retry();
      return;
    }
    this.ws = ws;

    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "redirect") {
        this._abandon(ws);
        this._open(msg.leader_url);
        return;
      }
      if (msg.type === "unavailable") {
        this._abandon(ws);
        this.onState({ state: "election" });
        this._retry();
        return;
      }
      if (msg.type === "welcome") {
        this.attempt = 0;
        this.nodeId = msg.node_id;
        this.onState({ state: "connected", nodeId: msg.node_id, url });
      }
      this.onMessage(msg);
    };

    ws.onclose = () => {
      if (this.ws !== ws) return; // conexión ya descartada
      this.onState({ state: "reconnecting" });
      this._retry();
    };
  }

  _abandon(ws) {
    ws.onclose = null;
    ws.onmessage = null;
    ws.close();
  }

  _retry() {
    this.ws = null;
    this.attempt += 1;
    this.next = (this.next + 1) % this.servers.length;
    const delay = Math.min(300 * this.attempt, 3000); // backoff lineal acotado
    setTimeout(() => this._open(this.servers[this.next]), delay);
  }
}
