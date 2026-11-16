// Los nodos corren en la misma máquina que sirve este frontend. Se usa el host
// desde el que se abrió la página, así funciona tanto en http://localhost:8080
// como desde otra computadora de la red (http://192.168.x.x:8080).
export const HOST = window.location.hostname || "localhost";

// Direcciones públicas de los nodos del clúster. El cliente prueba con cada
// una hasta encontrar al líder (los seguidores responden con una redirección).
export const SERVERS = [8001, 8002, 8003].map((port) => `ws://${HOST}:${port}/ws`);

// Cada cuánto se consulta /status de cada nodo para el panel del clúster (ms).
export const STATUS_POLL_MS = 1500;
