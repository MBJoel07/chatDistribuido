# Chat Distribuido Multi-sala

Trabajo práctico de **Sistemas Distribuidos**: un chat con salas que corre sobre un
clúster de 3 nodos. Si el nodo líder se cae, los demás lo detectan, eligen un nuevo
líder con el **algoritmo Bully** y los clientes se reconectan solos, sin perder el
historial gracias a la **replicación** del estado.

## Arquitectura

```
                 Navegador (frontend HTML/JS)
                        │  WebSocket (JSON)
                        ▼
   ┌──────────┐   ┌──────────┐   ┌──────────┐
   │  Nodo 1  │◄─►│  Nodo 2  │◄─►│  Nodo 3  │   HTTP/JSON entre nodos:
   │ seguidor │   │ seguidor │   │  LÍDER   │   heartbeats, Bully, replicación
   └──────────┘   └──────────┘   └──────────┘
```

- **Cliente-servidor con primario-respaldo**: sólo el líder atiende clientes y
  ordena las escrituras. Si un cliente se conecta a un seguidor, recibe una
  redirección al líder.
- **Replicación**: cada operación (crear sala, enviar mensaje) tiene un índice
  global; el líder la aplica y la envía a los seguidores antes de confirmarla.
- **Detección de fallos**: los seguidores envían heartbeats al líder cada 1 s; sin
  respuesta durante 3 s, inician una elección.
- **Elección de líder**: algoritmo Bully. Gana el nodo vivo de mayor ID. Antes de
  asumir, adopta el estado más reciente disponible en el clúster.
- **Recuperación**: un nodo que vuelve copia el estado del líder actual (snapshot)
  y luego participa de la elección.
- **Concurrencia**: servidor asíncrono (`asyncio` + `aiohttp`); las escrituras se
  serializan con un lock para mantener un orden total por sala.

| Aspecto | Solución |
|---|---|
| Concurrencia | `asyncio`, una corrutina por cliente, lock de escritura |
| Elección | Bully (`backend/chat/election.py`) |
| Tolerancia a fallos | Heartbeats + timeout, reconexión con backoff en el cliente |
| Replicación / consistencia | Log de operaciones primario-respaldo, snapshots (`state.py`, `replication.py`) |
| Comunicación | WebSocket (clientes) y REST/JSON (entre nodos) |

## Estructura del repositorio

```
chatDistribuido/
├── README.md
├── docker-compose.yml        # 3 nodos + frontend
├── backend/                  # Servidor (Python 3.11+, aiohttp)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── main.py               # Punto de entrada de un nodo
│   ├── chat/
│   │   ├── config.py         # Configuración por variables de entorno
│   │   ├── state.py          # Estado replicado (salas, mensajes, log)
│   │   ├── node.py           # Nodo: roles, heartbeats, sincronización
│   │   ├── election.py       # Algoritmo Bully
│   │   ├── replication.py    # Replicación primario-respaldo
│   │   ├── peer_api.py       # API HTTP entre nodos + /status
│   │   └── clients.py        # WebSocket de clientes y protocolo
│   └── tests/
│       └── test_state.py     # Tests unitarios
├── frontend/                 # Cliente web (HTML + CSS + JS, sin build)
│   ├── index.html
│   ├── css/styles.css
│   └── js/
│       ├── config.js         # URLs de los nodos
│       ├── connection.js     # Conexión con redirección y reconexión
│       └── app.js            # Interfaz y panel de estado del clúster
├── scripts/
│   ├── run_local.sh          # Levanta todo sin Docker
│   └── pruebas.py            # Casos de prueba: normal, carga, fallo
└── docs/                     # Informe y diagramas
```

## Cómo ejecutarlo

### Con Docker (recomendado)

```bash
docker compose up --build
```

Abrí http://localhost:8080 en dos pestañas con nombres distintos.

### Sin Docker

```bash
pip install -r backend/requirements.txt
./scripts/run_local.sh
```

En Windows sin Bash, abrí 3 terminales dentro de `backend/` (PowerShell):

```powershell
$env:NODE_ID=1; $env:PORT=8001; $env:PEERS="2=http://localhost:8002,3=http://localhost:8003"; python main.py
$env:NODE_ID=2; $env:PORT=8002; $env:PEERS="1=http://localhost:8001,3=http://localhost:8003"; python main.py
$env:NODE_ID=3; $env:PORT=8003; $env:PEERS="1=http://localhost:8001,2=http://localhost:8002"; python main.py
```

y en otra, dentro de `frontend/`: `python -m http.server 8080`.

## Pruebas

```bash
# Tests unitarios
cd backend && python -m pytest

# Con el clúster levantado:
python scripts/pruebas.py normal                       # 1. Flujo normal
python scripts/pruebas.py carga --clientes 20 --mensajes 10   # 2. Concurrencia
python scripts/pruebas.py fallo                        # 3. Caída del líder (usa docker compose stop)
```

Sin Docker, la prueba de fallo necesita saber cómo apagar el nodo, por ejemplo:
`python scripts/pruebas.py fallo --kill-cmd "pkill -f 'NODE_ID={id}'"` o apagarlo a mano.

### Demo de tolerancia a fallos

1. Abrí el frontend y mandá algunos mensajes; el panel derecho muestra al nodo 3 como líder.
2. `docker compose stop node3`
3. En ~3 s el nodo 2 pasa a líder, el chat se reconecta solo y el historial sigue ahí.
4. `docker compose start node3`: el nodo 3 se sincroniza y recupera el liderazgo (Bully).

## Protocolo cliente ↔ servidor (WebSocket, JSON)

| Cliente → servidor | Servidor → cliente |
|---|---|
| `{"type":"hello","user":"ana"}` | `{"type":"welcome","node_id":3}` |
| `{"type":"create_room","room":"x"}` | `{"type":"redirect","leader_url":"ws://…"}` |
| `{"type":"join","room":"x"}` | `{"type":"rooms","rooms":[…]}` / `{"type":"history",…}` |
| `{"type":"message","room":"x","text":"hola"}` | `{"type":"message","message":{…}}` / `{"type":"users",…}` |

## Limitaciones conocidas

- El estado vive en memoria (replicado en los 3 nodos, pero no persiste si se caen todos).
- Durante el cambio de líder hay una ventana de ~3 s sin servicio.
- Ante una partición de red podrían convivir dos líderes por un momento (Bully no
  usa quórum); se resuelve cuando vuelve la comunicación y gana el de mayor ID.
