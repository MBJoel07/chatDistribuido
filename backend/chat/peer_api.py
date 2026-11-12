"""API HTTP interna entre nodos del clúster (REST + JSON).

    GET  /peer/heartbeat    estado del nodo (lo usan los seguidores para vigilar al líder)
    POST /peer/election     mensaje ELECTION del algoritmo Bully
    POST /peer/coordinator  anuncio del nuevo líder (COORDINATOR)
    POST /peer/replicate    operación replicada desde el líder
    GET  /peer/state        snapshot completo del estado

Además expone GET /status (público, con CORS) para el panel del frontend.
"""
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from .node import Node

routes = web.RouteTableDef()


def _node(request: web.Request) -> "Node":
    return request.app["node"]


@routes.get("/peer/heartbeat")
async def heartbeat(request: web.Request) -> web.Response:
    return web.json_response(_node(request).status())


@routes.post("/peer/election")
async def election(request: web.Request) -> web.Response:
    node = _node(request)
    body = await request.json()
    ok = node.election.on_election_message(int(body["candidate_id"]))
    return web.json_response({"ok": ok, "node_id": node.id})


@routes.post("/peer/coordinator")
async def coordinator(request: web.Request) -> web.Response:
    node = _node(request)
    body = await request.json()
    await node.on_coordinator(int(body["leader_id"]), body["public_ws_url"])
    return web.json_response({"ok": True})


@routes.post("/peer/replicate")
async def replicate(request: web.Request) -> web.Response:
    node = _node(request)
    body = await request.json()
    result = await node.replicator.on_replicate(int(body["leader_id"]), body["op"])
    return web.json_response(result)


@routes.get("/peer/state")
async def state(request: web.Request) -> web.Response:
    return web.json_response(_node(request).state.snapshot())


@routes.get("/status")
async def public_status(request: web.Request) -> web.Response:
    node = _node(request)
    data = node.status() | {
        "clients": len(node.clients.clients),
        "rooms": node.state.room_summary(),
    }
    return web.json_response(data, headers={"Access-Control-Allow-Origin": "*"})
