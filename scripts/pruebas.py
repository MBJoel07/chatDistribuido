"""Casos de prueba del informe, ejecutados contra el clúster en marcha.

    python scripts/pruebas.py normal    # 1. Flujo normal
    python scripts/pruebas.py carga     # 2. Carga / concurrencia (N clientes simultáneos)
    python scripts/pruebas.py fallo     # 3. Caída del líder y recuperación

Requiere el clúster levantado (docker compose up, o scripts/run_local.sh).
Para la prueba de fallo, --kill-cmd indica cómo apagar un nodo; {id} se
reemplaza por el ID del líder. Por defecto: "docker compose stop node{id}".
Con --kill-cmd manual, el script espera a que lo apagues vos (Ctrl+C).
"""
import argparse
import asyncio
import json
import subprocess
import sys
import time
import uuid

import aiohttp

SERVERS = ["ws://localhost:8001/ws", "ws://localhost:8002/ws", "ws://localhost:8003/ws"]


class Cliente:
    """Cliente mínimo: se conecta al líder siguiendo redirecciones y reintenta."""

    def __init__(self, session: aiohttp.ClientSession, user: str) -> None:
        self.session = session
        self.user = user
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.node_id: int | None = None

    async def conectar(self, intentos: int = 20) -> int:
        for intento in range(intentos):
            for url in SERVERS:
                try:
                    ws = await self.session.ws_connect(url, timeout=2)
                except (aiohttp.ClientError, OSError):
                    continue
                first = await ws.receive_json(timeout=3)
                if first["type"] == "redirect":
                    await ws.close()
                    try:  # el líder indicado puede estar caído y aún no detectado
                        ws = await self.session.ws_connect(first["leader_url"], timeout=2)
                    except (aiohttp.ClientError, OSError):
                        continue
                    first = await ws.receive_json(timeout=3)
                if first["type"] == "welcome":
                    self.ws, self.node_id = ws, first["node_id"]
                    await self.enviar({"type": "hello", "user": self.user})
                    await self.esperar("rooms")
                    return self.node_id
                await ws.close()
            await asyncio.sleep(min(0.5 * (intento + 1), 3))  # backoff
        raise RuntimeError("No se pudo conectar a ningún líder")

    async def enviar(self, payload: dict) -> None:
        await self.ws.send_json(payload)

    async def esperar(self, tipo: str, timeout: float = 5, cond=lambda m: True) -> dict:
        fin = time.monotonic() + timeout
        while True:
            restante = fin - time.monotonic()
            if restante <= 0:
                raise TimeoutError(f"No llegó un mensaje '{tipo}'")
            msg = await self.ws.receive(timeout=restante)
            if msg.type != aiohttp.WSMsgType.TEXT:
                raise ConnectionError("Conexión cerrada")
            data = json.loads(msg.data)
            if data["type"] == tipo and cond(data):
                return data

    async def unirse(self, sala: str) -> list[dict]:
        await self.enviar({"type": "join", "room": sala})
        return (await self.esperar("history", cond=lambda m: m["room"] == sala))["messages"]

    async def decir(self, sala: str, texto: str) -> dict:
        await self.enviar({"type": "message", "room": sala, "text": texto})
        return await self.esperar("message", cond=lambda m: m["message"]["text"] == texto)

    async def cerrar(self) -> None:
        if self.ws:
            await self.ws.close()


def ok(texto: str) -> None:
    print(f"  [OK] {texto}")


async def status(session: aiohttp.ClientSession) -> list[dict]:
    res = []
    for url in SERVERS:
        http = url.replace("ws://", "http://").replace("/ws", "/status")
        try:
            async with session.get(http, timeout=aiohttp.ClientTimeout(total=1)) as r:
                res.append(await r.json())
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            res.append({"url": http, "caido": True})
    return res


# ---------------------------------------------------------------------- #
async def prueba_normal(session: aiohttp.ClientSession) -> None:
    print("1) Flujo normal")
    sala = f"sala-{uuid.uuid4().hex[:6]}"
    ana, beto = Cliente(session, "ana"), Cliente(session, "beto")
    nodo = await ana.conectar()
    await beto.conectar()
    ok(f"Ambos clientes conectados al líder (nodo {nodo}) tras seguir la redirección")

    await ana.enviar({"type": "create_room", "room": sala})
    await ana.esperar("rooms", cond=lambda m: any(r["name"] == sala for r in m["rooms"]))
    ok(f"Sala '{sala}' creada")

    await ana.unirse(sala)
    await beto.unirse(sala)
    await ana.decir(sala, "hola beto")
    recibido = await beto.esperar("message", cond=lambda m: m["message"]["text"] == "hola beto")
    ok(f"beto recibió el mensaje de ana (id {recibido['message']['id']})")

    await asyncio.sleep(0.3)
    indices = {s["node_id"]: s["last_index"] for s in await status(session) if "node_id" in s}
    assert len(set(indices.values())) == 1, f"Réplicas desincronizadas: {indices}"
    ok(f"Las 3 réplicas tienen el mismo índice de log: {indices}")
    await ana.cerrar()
    await beto.cerrar()


async def prueba_carga(session: aiohttp.ClientSession, clientes: int, mensajes: int) -> None:
    print(f"2) Carga: {clientes} clientes x {mensajes} mensajes simultáneos")
    sala = f"carga-{uuid.uuid4().hex[:6]}"
    cs = [Cliente(session, f"user{i}") for i in range(clientes)]
    await asyncio.gather(*(c.conectar() for c in cs))
    await cs[0].enviar({"type": "create_room", "room": sala})
    await cs[0].esperar("rooms", cond=lambda m: any(r["name"] == sala for r in m["rooms"]))
    await asyncio.gather(*(c.unirse(sala) for c in cs))

    async def trabajar(c: Cliente) -> None:
        for j in range(mensajes):
            await c.decir(sala, f"{c.user}-{j}")

    inicio = time.perf_counter()
    await asyncio.gather(*(trabajar(c) for c in cs))
    dur = time.perf_counter() - inicio
    total = clientes * mensajes
    ok(f"{total} mensajes en {dur:.2f}s ({total / dur:.0f} msg/s)")

    hist = await cs[0].unirse(sala)
    ids = [m["id"] for m in hist[-total:]]
    assert ids == sorted(ids) and len(set(ids)) == len(ids), "Orden total roto"
    ok("Orden total dentro de la sala sin duplicados")
    await asyncio.gather(*(c.cerrar() for c in cs))


async def prueba_fallo(session: aiohttp.ClientSession, kill_cmd: str) -> None:
    print("3) Caída del líder")
    sala = f"fallo-{uuid.uuid4().hex[:6]}"
    ana = Cliente(session, "ana")
    lider = await ana.conectar()
    await ana.enviar({"type": "create_room", "room": sala})
    await ana.esperar("rooms", cond=lambda m: any(r["name"] == sala for r in m["rooms"]))
    await ana.unirse(sala)
    for i in range(3):
        await ana.decir(sala, f"antes-{i}")
    ok(f"3 mensajes enviados al líder (nodo {lider})")

    if kill_cmd == "manual":
        await asyncio.to_thread(
            input, f"  >> Apagá el nodo {lider} (Ctrl+C en su terminal) y presioná Enter acá... ")
    else:
        cmd = kill_cmd.format(id=lider)
        print(f"  Apagando el líder: {cmd}")
        subprocess.run(cmd, shell=True, check=True)
    t0 = time.perf_counter()

    nuevo = await ana.conectar(intentos=30)
    ok(f"Reconectado al nuevo líder (nodo {nuevo}) en {time.perf_counter() - t0:.1f}s")
    assert nuevo != lider

    hist = await ana.unirse(sala)
    textos = [m["text"] for m in hist]
    assert textos == ["antes-0", "antes-1", "antes-2"], textos
    ok("El historial previo a la caída se conservó (replicación)")
    await ana.decir(sala, "despues")
    ok("Se puede seguir chateando con el nuevo líder")
    await ana.cerrar()


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("caso", choices=["normal", "carga", "fallo"])
    p.add_argument("--clientes", type=int, default=20)
    p.add_argument("--mensajes", type=int, default=10)
    p.add_argument("--kill-cmd", default="docker compose stop node{id}")
    args = p.parse_args()

    async with aiohttp.ClientSession() as session:
        try:
            if args.caso == "normal":
                await prueba_normal(session)
            elif args.caso == "carga":
                await prueba_carga(session, args.clientes, args.mensajes)
            else:
                await prueba_fallo(session, args.kill_cmd)
        except (AssertionError, TimeoutError, RuntimeError) as e:
            print(f"  [FALLÓ] {type(e).__name__}: {e}")
            return 1
    print("  Prueba superada.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
