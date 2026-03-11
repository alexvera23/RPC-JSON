import socket
import json
import base64
import sys
import os

ARCHIVO_ESTADO = "estado_red.json"

def cargar_estado():
    # LISTA DE NODOS SEMILLA (Bootstrappers)
    nodos_semilla_por_defecto = [
        ["172.26.166.127", 6001],
        ["172.26.166.127", 6002],
        ["192.168.1.76", 6003]
    ]
    
    estado = {"indice_actual": 0, "nodos": nodos_semilla_por_defecto}
    if os.path.exists(ARCHIVO_ESTADO):
        try:
            with open(ARCHIVO_ESTADO, "r") as f:
                datos_guardados = json.loads(f.read())
                if len(datos_guardados.get("nodos", [])) > 0:
                    estado = datos_guardados
        except: pass
    return estado

def guardar_estado(estado):
    with open(ARCHIVO_ESTADO, "w") as f:
        f.write(json.dumps(estado, indent=4))

def recibir_mensaje(conn):
    buffer = b""
    while True:
        chunk = conn.recv(65536)
        if not chunk: break
        buffer += chunk
        if b"<EOF>" in buffer: break
    if not buffer: return None
    return buffer.split(b"<EOF>")[0]

def enviar_mensaje(conn, diccionario_json):
    mensaje_bytes = json.dumps(diccionario_json).encode('utf-8')
    conn.sendall(mensaje_bytes + b"<EOF>")

def sincronizar_mapa_de_red(ip, puerto, estado_local):
    payload = {"jsonrpc": "2.0", "method": "get_nodes", "id": 1}
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2) # Tiempo corto solo para preguntar el estado
            s.connect((ip, puerto))
            enviar_mensaje(s, payload)
            data = recibir_mensaje(s)
            
            if data:
                res = json.loads(data.decode('utf-8'))
                if 'result' in res and 'nodes' in res['result']:
                    nodos_vivos = res['result']['nodes']
                    estado_local["nodos"] = nodos_vivos
                    estado_local["indice_actual"] = estado_local["indice_actual"] % len(nodos_vivos)
                    return True
    except:
        return False

def enviar_archivo(ruta_archivo):
    if not os.path.exists(ruta_archivo):
        print("El archivo no existe.")
        return

    # 1. Cargar la red conocida
    estado = cargar_estado()
    nodos_conocidos = estado["nodos"]

    # 2. Preparar el archivo (se hace una sola vez aunque haya reintentos)
    print(f"[CLIENTE] Codificando archivo pesado. Esto puede tardar unos segundos...")
    with open(ruta_archivo, "rb") as f:
        contenido_b64 = base64.b64encode(f.read()).decode('utf-8')

    exito = False
    
    # BUCLE DE TOLERANCIA A FALLOS
    while nodos_conocidos and not exito:
        indice = estado["indice_actual"] % len(nodos_conocidos)
        ip_destino, puerto_destino = nodos_conocidos[indice]
        
        print(f"\n[CLIENTE] Contactando Nodo: {ip_destino}:{puerto_destino}...")

        # 3. Auto-Descubrimiento (Gossip Sync)
        if sincronizar_mapa_de_red(ip_destino, puerto_destino, estado):
            nodos_conocidos = estado["nodos"]
        else:
            print(f"   [!] El nodo {ip_destino}:{puerto_destino} no responde. Buscando alternativa...")
            nodos_conocidos.pop(indice)
            if nodos_conocidos:
                estado["nodos"] = nodos_conocidos
                estado["indice_actual"] = 0
                guardar_estado(estado)
            continue 

        # 4. Enviar Archivo
        ttl_dinamico = len(nodos_conocidos)
        payload_upload = {
            "jsonrpc": "2.0",
            "method": "upload",
            "params": {
                "filename": os.path.basename(ruta_archivo),
                "content": contenido_b64,
                "ttl": ttl_dinamico
            },
            "id": 2
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # --- SOLUCIÓN A LA ESCRITURA FANTASMA ---
                s.settimeout(3) # 1. Fase corta: Comprobar que la puerta está abierta
                s.connect((ip_destino, puerto_destino))
                
                s.settimeout(300) # 2. Fase larga (5 MINUTOS): Subir datos pesados
                print(f"   [->] Transmitiendo datos a la red...")
                enviar_mensaje(s, payload_upload)
                
                data = recibir_mensaje(s)
                if data:
                    respuesta = json.loads(data.decode('utf-8'))
                    if 'result' in respuesta:
                        print(f"[EXITO] {respuesta['result']}")
                        exito = True
                    elif 'error' in respuesta:
                        print(f"[ERROR DEL SISTEMA] {respuesta['error']['message']}")
                        exito = True 
                        
            # Si tuvo éxito, preparamos el índice para el próximo archivo
            if exito:
                estado["indice_actual"] = (estado["indice_actual"] + 1) % len(nodos_conocidos)
                guardar_estado(estado)
                break
                
        except socket.timeout:
            print(f"   [!] Se agotó el tiempo de espera. El archivo es muy pesado o la red es muy lenta.")
            # Lo quitamos para no seguir intentando con un nodo congestionado
            nodos_conocidos.pop(indice)
            if nodos_conocidos:
                estado["nodos"] = nodos_conocidos
                estado["indice_actual"] = 0
                guardar_estado(estado)
        except Exception as e:
            print(f"   [!] Conexión perdida con {puerto_destino}: {e}. Reintentando...")
            nodos_conocidos.pop(indice)
            if nodos_conocidos:
                estado["nodos"] = nodos_conocidos
                estado["indice_actual"] = 0
                guardar_estado(estado)

    if not exito:
        print("[FALLO TOTAL] No se pudo enviar el archivo. Red colapsada o inaccesible.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python cliente_dinamico.py <ruta_archivo>")
    else:
        enviar_archivo(sys.argv[1])