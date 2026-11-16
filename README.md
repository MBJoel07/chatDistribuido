# Chat Distribuido Multi-sala

Trabajo práctico de **Sistemas Distribuidos**: un chat con salas que corre sobre un
clúster de 3 nodos. Si el nodo líder se cae, los demás lo detectan, eligen un nuevo
líder con el **algoritmo Bully** y los clientes se reconectan solos, sin perder el
historial gracias a la **replicación** del estado.

## Tecnologías

| Parte | Tecnología |
|---|---|
| Backend | **Python 3.10+** con `aiohttp` (única dependencia) y `asyncio` |
| Frontend | **HTML + CSS + JavaScript** puro (sin React, sin build) |
| Despliegue | Docker Compose (opcional) |
| Pruebas | `pytest` + script de pruebas de integración |

> **No usa Node.js ni `npm`.** No hay `npm install` ni `npm run dev`.
>
> **No tiene base de datos.** Los mensajes se guardan en memoria y se replican en
> los 3 nodos: si se cae uno, los datos siguen en los otros dos. Sólo se pierden
> si se apagan los 3 a la vez.

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
├── backend/                  # Servidor (Python, aiohttp)
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
│   ├── run_local.sh          # Levanta todo sin Docker (Linux/Mac/Git Bash)
│   └── pruebas.py            # Casos de prueba: normal, carga, fallo
└── docs/                     # Informe y diagramas
```

## Cómo levantarlo

### 1. Requisitos

Python 3.10 o superior. Para verificarlo:

```bash
python --version
```

### 2. Instalar la dependencia (una sola vez)

Desde la carpeta del repositorio:

```bash
pip install -r backend/requirements.txt
```

### 3. Levantar los 3 nodos y el frontend

**Opción A — Linux, Mac o Git Bash** (un solo comando, Ctrl+C detiene todo):

```bash
./scripts/run_local.sh
```

**Opción B — Windows con PowerShell** (4 terminales).

En las tres primeras, entrá a la carpeta `backend` y corré un nodo en cada una:

```powershell
$env:NODE_ID=1; $env:PORT=8001; $env:PEERS="2=http://localhost:8002,3=http://localhost:8003"; python main.py
```

```powershell
$env:NODE_ID=2; $env:PORT=8002; $env:PEERS="1=http://localhost:8001,3=http://localhost:8003"; python main.py
```

```powershell
$env:NODE_ID=3; $env:PORT=8003; $env:PEERS="1=http://localhost:8001,2=http://localhost:8002"; python main.py
```

En la cuarta, entrá a la carpeta `frontend`:

```powershell
python -m http.server 8080
```

**Opción C — Docker** (si lo tenés instalado):

```bash
docker compose up --build
```

### 4. Abrir el chat

Entrá a http://localhost:8080

El estado de cada nodo también se puede ver en http://localhost:8001/status,
http://localhost:8002/status y http://localhost:8003/status.

## Cómo probarlo

### Prueba manual (la de la demo)

1. Abrí dos pestañas (una normal y otra en incógnito) y entrá con nombres
   distintos, por ejemplo "ana" y "beto".
2. Creá una sala y escribí desde las dos: los mensajes aparecen en ambas.
3. En el panel derecho, el **nodo 3** figura como **Líder**.
4. Apagá el nodo 3: **Ctrl+C** en su terminal (o `docker compose stop node3`).
5. En unos 3 segundos el panel muestra al nodo 3 como **Caído** y al **nodo 2**
   como **Líder**. El chat avisa "Reconectado al nuevo líder: nodo 2".
6. Seguí escribiendo: el chat funciona y los mensajes anteriores siguen ahí.
7. Volvé a levantar el nodo 3 (mismo comando, o `docker compose start node3`):
   se sincroniza y recupera el liderazgo, como indica Bully.

### Pruebas automáticas

Con el clúster levantado, desde la carpeta del repositorio:

```bash
python scripts/pruebas.py normal
```

```bash
python scripts/pruebas.py carga --clientes 20 --mensajes 10
```

```bash
python scripts/pruebas.py fallo --kill-cmd manual
```

| Prueba | Qué verifica |
|---|---|
| `normal` | Dos clientes en una sala se ven entre sí y los 3 nodos quedan con el mismo estado |
| `carga` | Muchos clientes escribiendo a la vez: todos los mensajes llegan, en orden y sin duplicados |
| `fallo` | Al caer el líder se elige uno nuevo, el cliente se reconecta y el historial se conserva |

En la prueba de fallo con `--kill-cmd manual`, el script te dice qué nodo es el
líder, lo apagás con Ctrl+C y presionás Enter. Con Docker podés omitir esa opción:
el script lo apaga solo con `docker compose stop`.

### Tests unitarios

Desde la carpeta `backend`:

```bash
pip install -r requirements-dev.txt
```

```bash
python -m pytest
```

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
