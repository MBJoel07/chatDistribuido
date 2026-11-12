"""Punto de entrada de un nodo del chat distribuido.

Uso local:
    NODE_ID=1 PORT=8001 PEERS=2=http://localhost:8002,3=http://localhost:8003 python main.py
"""
import logging

from aiohttp import web

from chat.clients import websocket_handler
from chat.config import Config
from chat.node import Node
from chat.peer_api import routes


def create_app(config: Config) -> web.Application:
    app = web.Application()
    node = Node(config)
    app["node"] = node
    app.add_routes(routes)
    app.router.add_get("/ws", websocket_handler)

    async def on_startup(_app: web.Application) -> None:
        await node.start()

    async def on_shutdown(_app: web.Application) -> None:
        # Cerrar los WebSockets antes de que aiohttp espere a que terminen;
        # si no, ante un SIGTERM (p. ej. "docker compose stop") los clientes
        # quedarían colgados hasta 60 s sin enterarse de la caída.
        await node.clients.close_all()

    async def on_cleanup(_app: web.Application) -> None:
        await node.stop()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    config = Config.from_env()
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [nodo {config.node_id}] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    web.run_app(create_app(config), host=config.host, port=config.port, print=None, shutdown_timeout=2)


if __name__ == "__main__":
    main()
