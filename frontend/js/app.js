import { SERVERS, STATUS_POLL_MS } from "./config.js";
import { ChatConnection } from "./connection.js";

const $ = (id) => document.getElementById(id);

const state = {
  user: null,
  room: "general",
  rooms: [],
  lastNodeId: null,
};

let conn = null;

// ------------------------------------------------------------------ //
// Ingreso
// ------------------------------------------------------------------ //
$("login-form").addEventListener("submit", (e) => {
  e.preventDefault();
  state.user = $("username").value.trim() || "anónimo";
  $("login").hidden = true;
  $("app").hidden = false;
  $("me").textContent = state.user;
  $("msg-text").focus();

  conn = new ChatConnection(SERVERS, { onMessage, onState });
  conn.start();
  pollCluster();
});

// ------------------------------------------------------------------ //
// Mensajes del servidor
// ------------------------------------------------------------------ //
function onMessage(msg) {
  switch (msg.type) {
    case "welcome":
      // Al (re)conectar: identificarse y volver a la sala en la que estábamos.
      conn.send({ type: "hello", user: state.user });
      conn.send({ type: "join", room: state.room });
      break;
    case "rooms":
      state.rooms = msg.rooms;
      renderRooms();
      break;
    case "history":
      if (msg.room !== state.room) return;
      $("messages").replaceChildren(...msg.messages.map(messageItem));
      scrollToBottom();
      break;
    case "message":
      if (msg.message.room !== state.room) return;
      $("messages").append(messageItem(msg.message));
      scrollToBottom();
      break;
    case "users":
      if (msg.room === state.room) {
        $("room-users").textContent = `${msg.users.length} en línea: ${msg.users.join(", ")}`;
      }
      break;
    case "error":
      systemNote(`Error: ${msg.reason}`);
      break;
  }
}

function onState(s) {
  const el = $("conn");
  el.className = `conn conn-${s.state}`;
  if (s.state === "connected") {
    el.textContent = `Conectado · nodo ${s.nodeId} (líder)`;
    if (state.lastNodeId !== null && state.lastNodeId !== s.nodeId) {
      // Se mostrará después de que llegue el historial de la sala.
      setTimeout(() => systemNote(`Reconectado al nuevo líder: nodo ${s.nodeId}`), 200);
    }
    state.lastNodeId = s.nodeId;
  } else if (s.state === "reconnecting") {
    el.textContent = "Conexión perdida · reconectando…";
  } else if (s.state === "election") {
    el.textContent = "Elección de líder en curso…";
  } else {
    el.textContent = "Conectando…";
  }
}

// ------------------------------------------------------------------ //
// Acciones del usuario
// ------------------------------------------------------------------ //
$("msg-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const input = $("msg-text");
  const text = input.value.trim();
  if (!text) return;
  if (conn.send({ type: "message", room: state.room, text })) {
    input.value = "";
  } else {
    systemNote("Sin conexión: el mensaje no se envió");
  }
});

$("room-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const input = $("room-name");
  const room = input.value.trim();
  if (!room) return;
  conn.send({ type: "create_room", room });
  input.value = "";
  joinRoom(room);
});

function joinRoom(room) {
  state.room = room;
  $("room-title").textContent = `# ${room}`;
  $("room-users").textContent = "";
  $("messages").replaceChildren();
  conn.send({ type: "join", room });
  renderRooms();
}

// ------------------------------------------------------------------ //
// Renderizado
// ------------------------------------------------------------------ //
function renderRooms() {
  const items = state.rooms.map((r) => {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = r.name === state.room ? "room active" : "room";
    btn.innerHTML = `<span class="room-name"></span><span class="count"></span>`;
    btn.querySelector(".room-name").textContent = `# ${r.name}`;
    btn.querySelector(".count").textContent = r.messages;
    btn.addEventListener("click", () => joinRoom(r.name));
    li.append(btn);
    return li;
  });
  $("rooms").replaceChildren(...items);
}

function messageItem(m) {
  const li = document.createElement("li");
  li.className = m.user === state.user ? "msg mine" : "msg";
  const time = new Date(m.ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  li.innerHTML = `<div class="meta"><strong></strong><span></span></div><p></p>`;
  li.querySelector("strong").textContent = m.user;
  li.querySelector("span").textContent = `${time} · #${m.id} · nodo ${m.node}`;
  li.querySelector("p").textContent = m.text;
  return li;
}

function systemNote(text) {
  const li = document.createElement("li");
  li.className = "msg system";
  li.textContent = text;
  $("messages").append(li);
  scrollToBottom();
}

function scrollToBottom() {
  const box = $("messages");
  box.scrollTop = box.scrollHeight;
}

// ------------------------------------------------------------------ //
// Panel de estado del clúster (consulta /status de cada nodo)
// ------------------------------------------------------------------ //
async function fetchStatus(wsUrl) {
  const url = wsUrl.replace(/^ws/, "http").replace(/\/ws$/, "/status");
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 1000);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function pollCluster() {
  const statuses = await Promise.all(SERVERS.map(fetchStatus));
  const items = statuses.map((s, i) => {
    const li = document.createElement("li");
    let role, cls;
    if (!s) { role = "Caído"; cls = "down"; }
    else if (s.is_leader) { role = "Líder"; cls = "leader"; }
    else if (s.election_running || s.leader_id === null) { role = "En elección"; cls = "election"; }
    else { role = "Seguidor"; cls = "follower"; }

    li.className = `node ${cls}`;
    li.innerHTML = `<div class="node-head"><strong></strong><span class="badge"></span></div><span class="muted small"></span>`;
    li.querySelector("strong").textContent = `Nodo ${s ? s.node_id : i + 1}`;
    li.querySelector(".badge").textContent = role;
    li.querySelector(".small").textContent = s
      ? `log: ${s.last_index} ops · clientes: ${s.clients}`
      : "sin respuesta";
    return li;
  });
  $("nodes").replaceChildren(...items);
  setTimeout(pollCluster, STATUS_POLL_MS);
}
