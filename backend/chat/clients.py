"""Conexiones de los clientes (navegadores) por WebSocket.

Protocolo (mensajes JSON):

Cliente -> servidor
    {"type": "hello", "user": "ana"}
    {"type": "list_rooms"}
    {"type": "create_room", "room": "deportes"}
    {"type": "join", "room": "general"}
    {"type": "message", "room": "general", "text": "hola"}

Servidor -> cliente
    {"type": "welcome", "node_id": 3}
    {"type": "redirect", "leader_url": "ws://...", "leader_id": 3}
    {"type": "unavailable", "reason": "..."}          (elección en curso)
    {"type": "rooms", "rooms": [{"name": "general", "messages": 4}]}
    {"type": "history", "room": "general", "messages": [...]}
    {"type": "message", "message": {...}}
    {"type": "users", "room": "general", "users": ["ana", "beto"]}
    {"type": "error", "reason": "..."}
"""
import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import WSMsgType, web

from .state import StateError

if TYPE_CHECKING:
    from .node import Node

log = logging.getLogger("chat.clients")
MAX_USER = 24


class ClientConn:
    def __init__(self, ws: web.WebSocketResponse) -> None:
        self.ws = ws
        self.user: str | None = None
        self.room: str | None = None

    async def send(self, payload: dict[str, Any]) -> None:
        if not self.ws.closed:
            try:
                await self.ws.send_json(payload)
            except (ConnectionError, RuntimeError):
                pass  # el cliente se desconectó mientras le enviábamos


class ClientRegistry:
    """Clientes conectados a ESTE nodo (sólo el líder tiene clientes)."""

    def __init__(self) -> None:
        self.clients: set[ClientConn] = set()

    def add(self, c: ClientConn) -> None:
        self.clients.add(c)

    def remove(self, c: ClientConn) -> None:
        self.clients.discard(c)

    def users_in(self, room: str) -> list[str]:
        return sorted({c.user for c in self.clients if c.room == room and c.user})

    async def broadcast(self, payload: dict[str, Any], room: str | None = None) -> None:
        targets = [c for c in self.clients if room is None or c.room == room]
        await asyncio.gather(*(c.send(payload) for c in targets))

    async def broadcast_users(self, room: str) -> None:
        await self.broadcast({"type": "users", "room": room, "users": self.users_in(room)}, room)

    async def redirect_all(self, leader_url: str, leader_id: int) -> None:
        payload = {"type": "redirect", "leader_url": leader_url, "leader_id": leader_id}
        for c in list(self.clients):
            await c.send(payload)
            await c.ws.close()
        self.clients.clear()

    async def close_all(self) -> None:
        for c in list(self.clients):
            await c.ws.close()
        self.clients.clear()


# ---------------------------------------------------------------------- #
# Handler HTTP -> WebSocket
# ---------------------------------------------------------------------- #
def _not_leader_payload(node: "Node") -> dict[str, Any]:
    if node.leader_ws_url and not node.is_leader:
        return {"type": "redirect", "leader_url": node.leader_ws_url, "leader_id": node.leader_id}
    return {"type": "unavailable", "reason": "Elección de líder en curso, reintentá en unos segundos"}


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    node: "Node" = request.app["node"]
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)

    # Sólo el líder atiende clientes; los seguidores los redirigen.
    if not node.is_leader:
        await ws.send_json(_not_leader_payload(node))
        await ws.close()
        return ws

    client = ClientConn(ws)
    node.clients.add(client)
    await client.send({"type": "welcome", "node_id": node.id})
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
                await _handle(node, client, data)
            except StateError as e:
                await client.send({"type": "error", "reason": str(e)})
            except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                await client.send({"type": "error", "reason": "Mensaje mal formado"})
    finally:
        node.clients.remove(client)
        if client.room:
            await node.clients.broadcast_users(client.room)
    return ws


async def _handle(node: "Node", client: ClientConn, data: dict[str, Any]) -> None:
    kind = data["type"]

    if kind == "hello":
        client.user = (str(data.get("user", "")).strip() or "anónimo")[:MAX_USER]
        await client.send({"type": "rooms", "rooms": node.state.room_summary()})

    elif kind == "list_rooms":
        await client.send({"type": "rooms", "rooms": node.state.room_summary()})

    elif kind == "join":
        room = str(data["room"])
        history = node.state.history(room)  # valida que exista
        previous, client.room = client.room, room
        await client.send({"type": "history", "room": room, "messages": history})
        if previous and previous != room:
            await node.clients.broadcast_users(previous)
        await node.clients.broadcast_users(room)

    elif kind == "create_room":
        await _write(node, client, lambda: node.state.new_create_room_op(str(data["room"])))
        await node.clients.broadcast({"type": "rooms", "rooms": node.state.room_summary()})

    elif kind == "message":
        if not client.user:
            raise StateError("Primero enviá 'hello' con tu nombre")
        op = await _write(node, client, lambda: node.state.new_message_op(
            str(data["room"]), client.user, str(data["text"]), node.id))
        if op:
            await node.clients.broadcast({"type": "message", "message": op["message"]},
                                         room=op["message"]["room"])

    else:
        raise StateError(f"Tipo de mensaje desconocido: {kind}")


async def _write(node: "Node", client: ClientConn, build_op) -> dict[str, Any] | None:
    """Escritura en el líder: crea la operación, la aplica y la replica (en orden)."""
    async with node.write_lock:
        if not node.is_leader:
            await client.send(_not_leader_payload(node))
            return None
        op = build_op()
        node.state.apply(op)
        await node.replicator.replicate(op)
        return op
