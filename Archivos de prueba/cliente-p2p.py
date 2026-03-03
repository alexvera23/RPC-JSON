import socket
import json
import base64
import sys
import os

# LA RED CONOCIDA POR EL CLIENTE
NODOS_CONOCIDOS = [
    ('192.168.1.74', 6001), 
    ('192.168.1.76', 6002),
    ('192.168.1.76', 6003)
]

# El TTL dinámico es igual al número total de nodos
TTL_DINAMICO = len(NODOS_CONOCIDOS) 
ARCHIVO_ESTADO = ".estado_nodo.txt"

def obtener_siguiente_nodo():
    # Leer el último índice usado
    indice = 0
    if os.path.exists(ARCHIVO_ESTADO):
        try:
            with open(ARCHIVO_ESTADO, "r") as f:
                indice = int(f.read().strip())
        except:
            pass

    # Calcular el siguiente índice (Round-Robin)
    siguiente_indice = (indice + 1) % len(NODOS_CONOCIDOS)
    
    # Guardar el nuevo índice para la próxima ejecución
    with open(ARCHIVO_ESTADO, "w") as f:
        f.write(str(siguiente_indice))
        
    return NODOS_CONOCIDOS[indice]

def recibir_mensaje(conn):
    buffer = b""
    while True:
        chunk = conn.recv(65536)
        if not chunk: break
        buffer += chunk
        if b"<EOF>" in buffer: break
    if not buffer: return None
    return buffer.split(b"<EOF>")[0]

def enviar_archivo(ruta_archivo):
    if not os.path.exists(ruta_archivo):
        print("El archivo no existe.")
        return

    with open(ruta_archivo, "rb") as f:
        contenido_b64 = base64.b64encode(f.read()).decode('utf-8')

    # EL PAYLOAD LLEVA EL TTL DINÁMICO
    payload = {
        "jsonrpc": "2.0",
        "method": "upload",
        "params": {
            "filename": os.path.basename(ruta_archivo),
            "content": contenido_b64,
            "ttl": TTL_DINAMICO # <--- TTL dinámico basado en la red
        },
        "id": 1
    }

    # El cliente decide a qué nodo ir (Descentralizado)
    ip_destino, puerto_destino = obtener_siguiente_nodo()
    print(f"[CLIENTE] Conectando al Nodo de entrada: {puerto_destino} (TTL asignado: {TTL_DINAMICO})")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip_destino, puerto_destino))
            mensaje_json = json.dumps(payload).encode('utf-8')
            s.sendall(mensaje_json + b"<EOF>")
            
            data = recibir_mensaje(s)
            if data:
                respuesta = json.loads(data.decode('utf-8'))
                if 'result' in respuesta:
                    print(f"[EXITO] {respuesta['result']}")
                elif 'error' in respuesta:
                    print(f"[ERROR] {respuesta['error']['message']}")
                
    except ConnectionRefusedError:
        print(f"[ERROR] El Nodo {puerto_destino} está apagado. Ejecuta de nuevo para saltar al siguiente nodo.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python cliente_p2p.py <ruta_archivo>")
    else:
        enviar_archivo(sys.argv[1])