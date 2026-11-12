"""Elección de líder con el algoritmo Bully (Garcia-Molina, 1982).

1. El nodo que detecta la caída del líder envía ELECTION a todos los nodos
   con ID mayor.
2. Si ninguno responde, gana: se proclama líder y envía COORDINATOR a todos.
3. Si alguno responde OK, ese nodo "mayor" toma la elección. El iniciador
   espera el COORDINATOR; si no llega a tiempo, reinicia la elección.
4. Quien recibe ELECTION de un nodo menor responde OK e inicia su propia
   elección. Así siempre gana el nodo vivo de mayor ID.
"""
import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .node import Node

log = logging.getLogger("chat.election")


class BullyElection:
    def __init__(self, node: "Node") -> None:
        self.node = node
        self.running = False
        self._coordinator = asyncio.Event()

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        try:
            while True:
                self._coordinator.clear()
                higher = [pid for pid in self.node.peers if pid > self.node.id]
                log.info("ELECTION enviado a nodos mayores: %s", higher or "ninguno")

                responses = await asyncio.gather(*(
                    self.node.call_peer(pid, "POST", "/peer/election",
                                        {"candidate_id": self.node.id})
                    for pid in higher
                ))
                alive_higher = [pid for pid, r in zip(higher, responses) if r and r.get("ok")]

                if not alive_higher:
                    # Nadie mayor está vivo: este nodo gana.
                    await self.node.become_leader()
                    return

                log.info("Respondieron OK los nodos %s: espero COORDINATOR", alive_higher)
                try:
                    await asyncio.wait_for(self._coordinator.wait(),
                                           timeout=self.node.config.election_timeout)
                    return
                except asyncio.TimeoutError:
                    log.warning("No llegó COORDINATOR a tiempo: reinicio la elección")
        finally:
            self.running = False

    def on_election_message(self, candidate_id: int) -> bool:
        """Mensaje ELECTION de otro nodo. Devuelve True (OK) si este nodo es mayor."""
        if candidate_id < self.node.id:
            log.info("ELECTION recibido del nodo %s: respondo OK y tomo la elección", candidate_id)
            self.node.spawn(self.start())
            return True
        return False

    def coordinator_received(self) -> None:
        self._coordinator.set()
