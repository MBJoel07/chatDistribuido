// Direcciones públicas de los nodos del clúster. El cliente prueba con cada
// una hasta encontrar al líder (los seguidores responden con una redirección).
export const SERVERS = [
  "ws://localhost:8001/ws",
  "ws://localhost:8002/ws",
  "ws://localhost:8003/ws",
];

// Cada cuánto se consulta /status de cada nodo para el panel del clúster (ms).
export const STATUS_POLL_MS = 1500;
