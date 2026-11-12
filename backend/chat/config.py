"""Configuración del nodo, leída desde variables de entorno.

Ejemplo:
    NODE_ID=1
    PORT=8000
    PEERS=2=http://node2:8000,3=http://node3:8000
    PUBLIC_WS_URL=ws://localhost:8001/ws
"""
import os
from dataclasses import dataclass, field


def _parse_peers(raw: str) -> dict[int, str]:
    """Convierte "2=http://a:8000,3=http://b:8000" en {2: "http://a:8000", 3: ...}."""
    peers: dict[int, str] = {}
    for item in filter(None, (p.strip() for p in raw.split(","))):
        node_id, url = item.split("=", 1)
        peers[int(node_id)] = url.rstrip("/")
    return peers


@dataclass
class Config:
    node_id: int
    host: str
    port: int
    public_ws_url: str
    peers: dict[int, str] = field(default_factory=dict)

    # Tiempos (en segundos) del detector de fallos y de la elección.
    heartbeat_interval: float = 1.0   # cada cuánto un seguidor consulta al líder
    heartbeat_timeout: float = 3.0    # sin respuesta durante este tiempo => líder caído
    election_timeout: float = 2.0     # espera del mensaje COORDINATOR tras recibir un OK
    rpc_timeout: float = 0.8          # timeout de cada llamada HTTP entre nodos

    @classmethod
    def from_env(cls) -> "Config":
        node_id = int(os.environ.get("NODE_ID", "1"))
        port = int(os.environ.get("PORT", "8000"))
        return cls(
            node_id=node_id,
            host=os.environ.get("HOST", "0.0.0.0"),
            port=port,
            public_ws_url=os.environ.get("PUBLIC_WS_URL", f"ws://localhost:{port}/ws"),
            peers=_parse_peers(os.environ.get("PEERS", "")),
            heartbeat_interval=float(os.environ.get("HEARTBEAT_INTERVAL", "1.0")),
            heartbeat_timeout=float(os.environ.get("HEARTBEAT_TIMEOUT", "3.0")),
            election_timeout=float(os.environ.get("ELECTION_TIMEOUT", "2.0")),
            rpc_timeout=float(os.environ.get("RPC_TIMEOUT", "0.8")),
        )
