"""Nodo del clúster: rol (líder/seguidor), detector de fallos y sincronización.

Cada nodo corre el mismo código. En todo momento hay como máximo un líder,
que es el único que acepta clientes y ordena las escrituras. Los seguidores
mantienen una copia del estado y vigilan al líder con heartbeats; si deja de
responder, inician una elección (algoritmo Bully, ver election.py).
"""
import asyncio
import logging
import time
from typing import Any

import aiohttp

from .clients import ClientRegistry
from .config import Config
from .election import BullyElection
from .replication import Replicator
from .state import ChatState

log = logging.getLogger("chat.node")


class Node:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.id = config.node_id
        self.state = ChatState()
        self.clients = ClientRegistry()
        self.election = BullyElection(self)
        self.replicator = Replicator(self)

        self.leader_id: int | None = None
        self.leader_ws_url: str | None = None
        self.last_leader_contact = time.monotonic()

        # Serializa las escrituras del líder: asignar índice + aplicar + replicar.
        self.write_lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None
        self._tasks: set[asyncio.Task] = set()
        self._started_at = time.time()

    # ------------------------------------------------------------------ #
    # Propiedades y utilidades
    # ------------------------------------------------------------------ #
    @property
    def is_leader(self) -> bool:
        return self.leader_id == self.id

    @property
    def peers(self) -> dict[int, str]:
        return self.config.peers

    def spawn(self, coro) -> asyncio.Task:
        """Lanza una tarea en segundo plano guardando la referencia."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def status(self) -> dict[str, Any]:
        return {
            "node_id": self.id,
            "is_leader": self.is_leader,
            "leader_id": self.leader_id,
            "leader_ws_url": self.leader_ws_url,
            "public_ws_url": self.config.public_ws_url,
            "last_index": self.state.last_index,
            "election_running": self.election.running,
        }

    async def call_peer(self, peer_id: int, method: str, path: str,
                        payload: dict | None = None) -> dict | None:
        """Llamada HTTP a otro nodo. Devuelve el JSON o None si falló/timeout."""
        url = self.peers[peer_id] + path
        try:
            timeout = aiohttp.ClientTimeout(total=self.config.rpc_timeout)
            async with self.session.request(method, url, json=payload, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            return None

    # ------------------------------------------------------------------ #
    # Ciclo de vida
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        self.session = aiohttp.ClientSession()
        log.info("Nodo %s iniciado. Pares: %s", self.id, self.peers)
        await self._join_cluster()
        self.spawn(self._heartbeat_loop())

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await self.clients.close_all()
        if self.session:
            await self.session.close()

    async def _join_cluster(self) -> None:
        """Al arrancar (o recuperarse) busca un líder vigente y copia su estado.

        Luego inicia una elección, como indica Bully: si este nodo tiene el
        mayor ID vivo, tomará el liderazgo ya con el estado actualizado.
        """
        statuses = await asyncio.gather(
            *(self.call_peer(pid, "GET", "/peer/heartbeat") for pid in self.peers)
        )
        for st in statuses:
            if st and st.get("is_leader"):
                self.leader_id = st["node_id"]
                self.leader_ws_url = st["public_ws_url"]
                self.last_leader_contact = time.monotonic()
                await self.sync_from_leader()
                log.info("Líder actual encontrado: nodo %s", self.leader_id)
                break
        self.spawn(self.election.start())

    # ------------------------------------------------------------------ #
    # Detector de fallos (heartbeats)
    # ------------------------------------------------------------------ #
    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval)
            if self.is_leader or self.election.running:
                continue

            if self.leader_id is None:
                log.info("Sin líder conocido: inicio elección")
                await self.election.start()
                continue

            resp = await self.call_peer(self.leader_id, "GET", "/peer/heartbeat")
            now = time.monotonic()
            if resp and resp.get("is_leader"):
                self.last_leader_contact = now
                if resp["last_index"] > self.state.last_index:
                    # Nos perdimos alguna replicación: nos ponemos al día.
                    await self.sync_from_leader()
            elif now - self.last_leader_contact > self.config.heartbeat_timeout:
                log.warning("El líder (nodo %s) no responde: inicio elección", self.leader_id)
                self.leader_id = None
                self.leader_ws_url = None
                await self.election.start()

    # ------------------------------------------------------------------ #
    # Cambios de rol
    # ------------------------------------------------------------------ #
    async def become_leader(self) -> None:
        """Gana la elección: adopta el estado más reciente y anuncia COORDINATOR."""
        await self._adopt_most_recent_state()
        self.leader_id = self.id
        self.leader_ws_url = self.config.public_ws_url
        log.info("*** Nodo %s es el nuevo LÍDER (último índice %s) ***",
                 self.id, self.state.last_index)
        payload = {"leader_id": self.id, "public_ws_url": self.config.public_ws_url}
        await asyncio.gather(
            *(self.call_peer(pid, "POST", "/peer/coordinator", payload) for pid in self.peers)
        )

    async def _adopt_most_recent_state(self) -> None:
        """Si algún nodo vivo tiene operaciones que este no tiene, copia su estado.

        Evita que un líder nuevo con datos atrasados pise mensajes ya replicados.
        """
        statuses = await asyncio.gather(
            *(self.call_peer(pid, "GET", "/peer/heartbeat") for pid in self.peers)
        )
        best = max((s for s in statuses if s), key=lambda s: s["last_index"], default=None)
        if best and best["last_index"] > self.state.last_index:
            snap = await self.call_peer(best["node_id"], "GET", "/peer/state")
            if snap:
                self.state.load_snapshot(snap)
                log.info("Estado copiado del nodo %s (índice %s)", best["node_id"], snap["last_index"])

    async def on_coordinator(self, leader_id: int, leader_ws_url: str) -> None:
        """Recibe el anuncio de un nuevo líder."""
        if leader_id < self.id:
            # Regla de Bully: un nodo con ID mayor no acepta a uno menor como líder.
            log.info("Nodo %s se anunció líder pero tengo ID mayor: inicio elección", leader_id)
            self.spawn(self.election.start())
            return

        was_leader = self.is_leader
        self.leader_id = leader_id
        self.leader_ws_url = leader_ws_url
        self.last_leader_contact = time.monotonic()
        self.election.coordinator_received()
        log.info("Nuevo líder: nodo %s", leader_id)

        if was_leader and leader_id != self.id:
            log.info("Dejo de ser líder: redirijo a mis clientes al nodo %s", leader_id)
            await self.clients.redirect_all(leader_ws_url, leader_id)
        if leader_id != self.id:
            self.spawn(self.sync_from_leader())

    async def sync_from_leader(self) -> None:
        """Reemplaza el estado local por un snapshot del líder."""
        if self.leader_id is None or self.is_leader:
            return
        snap = await self.call_peer(self.leader_id, "GET", "/peer/state")
        if snap:
            self.state.load_snapshot(snap)
            log.info("Sincronizado con el líder (índice %s)", snap["last_index"])
