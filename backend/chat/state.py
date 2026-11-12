"""Estado replicado del chat: salas y mensajes.

El estado se modifica sólo aplicando *operaciones* numeradas con un índice
global creciente (log de operaciones). El líder asigna el índice y las
réplicas aplican las operaciones en el mismo orden, por lo que todos los
nodos terminan con el mismo estado (replicación primario-respaldo).
"""
import time
from typing import Any

DEFAULT_ROOM = "general"
MAX_ROOM_NAME = 32
MAX_TEXT = 1000


class StateError(Exception):
    """Operación inválida (sala inexistente, nombre vacío, etc.)."""


class ChatState:
    def __init__(self) -> None:
        self.rooms: dict[str, list[dict[str, Any]]] = {DEFAULT_ROOM: []}
        self.last_index = 0  # índice de la última operación aplicada

    # ------------------------------------------------------------------ #
    # Construcción de operaciones (sólo las usa el líder)
    # ------------------------------------------------------------------ #
    def new_create_room_op(self, room: str) -> dict[str, Any]:
        room = room.strip()
        if not room or len(room) > MAX_ROOM_NAME:
            raise StateError(f"El nombre de la sala debe tener entre 1 y {MAX_ROOM_NAME} caracteres")
        if room in self.rooms:
            raise StateError(f"La sala '{room}' ya existe")
        return {"index": self.last_index + 1, "op": "create_room", "room": room}

    def new_message_op(self, room: str, user: str, text: str, node_id: int) -> dict[str, Any]:
        text = text.strip()
        if room not in self.rooms:
            raise StateError(f"La sala '{room}' no existe")
        if not text:
            raise StateError("El mensaje está vacío")
        message = {
            "id": len(self.rooms[room]) + 1,  # orden total dentro de la sala
            "room": room,
            "user": user,
            "text": text[:MAX_TEXT],
            "ts": time.time(),
            "node": node_id,                  # nodo líder que lo ordenó
        }
        return {"index": self.last_index + 1, "op": "message", "message": message}

    # ------------------------------------------------------------------ #
    # Aplicación de operaciones (líder y réplicas)
    # ------------------------------------------------------------------ #
    def apply(self, op: dict[str, Any]) -> str:
        """Aplica una operación. Devuelve "applied", "duplicate" o "gap".

        - duplicate: ya se había aplicado (reintento), se ignora.
        - gap: faltan operaciones intermedias; la réplica debe pedir un snapshot.
        """
        index = op["index"]
        if index <= self.last_index:
            return "duplicate"
        if index > self.last_index + 1:
            return "gap"

        if op["op"] == "create_room":
            self.rooms.setdefault(op["room"], [])
        elif op["op"] == "message":
            msg = op["message"]
            self.rooms.setdefault(msg["room"], []).append(msg)
        else:
            raise StateError(f"Operación desconocida: {op['op']}")

        self.last_index = index
        return "applied"

    # ------------------------------------------------------------------ #
    # Snapshots (para nodos que se recuperan o que quedaron atrasados)
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict[str, Any]:
        return {"last_index": self.last_index, "rooms": self.rooms}

    def load_snapshot(self, snap: dict[str, Any]) -> None:
        self.rooms = {name: list(msgs) for name, msgs in snap["rooms"].items()}
        self.last_index = snap["last_index"]

    # ------------------------------------------------------------------ #
    # Consultas
    # ------------------------------------------------------------------ #
    def room_summary(self) -> list[dict[str, Any]]:
        return [{"name": name, "messages": len(msgs)} for name, msgs in sorted(self.rooms.items())]

    def history(self, room: str, limit: int = 100) -> list[dict[str, Any]]:
        if room not in self.rooms:
            raise StateError(f"La sala '{room}' no existe")
        return self.rooms[room][-limit:]
