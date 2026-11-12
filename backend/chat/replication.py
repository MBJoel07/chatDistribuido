"""Replicación primario-respaldo del log de operaciones.

El líder aplica cada operación localmente y la envía a todos los seguidores
antes de confirmarla al cliente (replicación sincrónica "best effort"): se
esperan las respuestas de los nodos vivos, con timeout para no bloquearse si
alguno está caído. Un seguidor que se atrasó lo detecta (hueco en el índice)
y pide un snapshot completo al líder.
"""
import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node import Node

log = logging.getLogger("chat.replication")


class Replicator:
    def __init__(self, node: "Node") -> None:
        self.node = node

    async def replicate(self, op: dict[str, Any]) -> int:
        """Envía la operación a los seguidores. Devuelve cuántos la confirmaron."""
        payload = {"leader_id": self.node.id, "op": op}
        peer_ids = list(self.node.peers)
        results = await asyncio.gather(*(
            self.node.call_peer(pid, "POST", "/peer/replicate", payload) for pid in peer_ids
        ))

        acks = 0
        for pid, res in zip(peer_ids, results):
            if res and res.get("ok"):
                acks += 1
            elif res and res.get("reason") == "stale_leader":
                # Otro nodo reconoce a un líder distinto: este nodo quedó desactualizado.
                log.warning("El nodo %s no me reconoce como líder: inicio elección", pid)
                self.node.spawn(self.node.election.start())
        log.debug("Operación %s replicada en %s/%s seguidores", op["index"], acks, len(peer_ids))
        return acks

    async def on_replicate(self, leader_id: int, op: dict[str, Any]) -> dict[str, Any]:
        """Seguidor: recibe una operación del líder y la aplica en orden."""
        node = self.node
        if node.leader_id is not None and leader_id != node.leader_id:
            if leader_id < node.leader_id:
                return {"ok": False, "reason": "stale_leader", "leader_id": node.leader_id}
            # Un líder de mayor ID cuyo COORDINATOR no recibimos: lo aceptamos.
            node.leader_id = leader_id

        result = node.state.apply(op)
        if result == "gap":
            log.info("Hueco en el log (tengo %s, llegó %s): pido snapshot",
                     node.state.last_index, op["index"])
            node.spawn(node.sync_from_leader())
            return {"ok": False, "reason": "gap"}
        return {"ok": True}
