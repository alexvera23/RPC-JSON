import socket
import json
import base64
import os
import threading
import sys
import random

LIMITE_ARCHIVOS_LOCALES = 5

def recibir_mensaje(conn):
    buffer = b""
    while True:
        chunk = conn.recv(65536)
        if not chunk: break
        buffer += chunk
        if b"<EOF>" in buffer: break
    if not buffer: return None
    return buffer.split(b"<EOF>")[0]

def contar_archivos(carpeta):
    return len([name for name in os.listdir(carpeta) if os.path.isfile(os.path.join(carpeta, name))])

def reenviar_a_vecino(payload_dict, vecinos, puerto_local):
    random.shuffle(vecinos)
    
    for ip_vecino, puerto_vecino in vecinos:
        try:
            print(f"      [NODO {puerto_local} -> NODO {puerto_vecino}] Reenviando paquete...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_vecino:
                s_vecino.connect((ip_vecino, puerto_vecino))
                mensaje_bytes = json.dumps(payload_dict).encode('utf-8')
                s_vecino.sendall(mensaje_bytes + b"<EOF>")
                
                data = recibir_mensaje(s_vecino)
                if data:
                    return json.loads(data.decode('utf-8'))
        except ConnectionRefusedError:
            print(f"      [x] Vecino {puerto_vecino} inactivo.")
            continue
            
    return {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Ningún vecino disponible."}}

def manejar_cliente(conn, addr, puerto_local, carpeta_raiz, vecinos):
    try:
        data = recibir_mensaje(conn)
        if not data: return
        
        request = json.loads(data.decode('utf-8'))
        metodo = request.get('method')
        params = request.get('params')
        id_req = request.get('id')
        
        response = {"jsonrpc": "2.0", "id": id_req}

        if metodo == 'upload':
            # Toma el TTL dinámico (por defecto asume 3 si por error no llega)
            ttl = params.get('ttl', 3) 
            nombre = params['filename']
            
            cantidad_actual = contar_archivos(carpeta_raiz)
            
            # CASO 1: HAY ESPACIO LOCAL
            if cantidad_actual < LIMITE_ARCHIVOS_LOCALES:
                print(f"[NODO {puerto_local}] Guardando '{nombre}' (Espacio: {cantidad_actual}/{LIMITE_ARCHIVOS_LOCALES})")
                datos_binarios = base64.b64decode(params['content'])
                ruta_final = os.path.join(carpeta_raiz, nombre)
                
                with open(ruta_final, 'wb') as f:
                    f.write(datos_binarios)
                response['result'] = f"Guardado en Nodo {puerto_local} ({len(datos_binarios)/1024/1024:.2f} MB)"
            
            # CASO 2: LLENO -> REBOTA A LA RED P2P
            else:
                if ttl > 1: # Si aún le queda vida al paquete
                    print(f"[NODO {puerto_local}] Lleno! Buscando ayuda en la red... (TTL restante: {ttl-1})")
                    params['ttl'] = ttl - 1 # Resta una vida al TTL dinámico
                    request['params'] = params
                    
                    response = reenviar_a_vecino(request, vecinos, puerto_local)
                else:
                    print(f"[NODO {puerto_local}] Lleno. El paquete murió (TTL=0).")
                    response['error'] = {"code": -32001, "message": "Red llena. No hay espacio en ningún nodo."}
        else:
            response['error'] = {"code": -32601, "message": "Metodo no encontrado"}

        mensaje_bytes = json.dumps(response).encode('utf-8')
        conn.sendall(mensaje_bytes + b"<EOF>")
            
    except Exception as e:
        print(f"Error en Nodo: {e}")
    finally:
        conn.close()

def iniciar_nodo(puerto_local, carpeta, vecinos):
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)
        
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', puerto_local))
    s.listen()
    print(f"[*] NODO P2P activo en puerto {puerto_local} | Límite: {LIMITE_ARCHIVOS_LOCALES}")
    print(f"[*] Vecinos registrados: {vecinos}")
    
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=manejar_cliente, args=(conn, addr, puerto_local, carpeta, vecinos))
        t.start()

if __name__ == "__main__":
    puerto_mio = int(sys.argv[1]) if len(sys.argv) > 1 else 6001
    carpeta_mia = f"datos_nodo_{puerto_mio}"
    
    # Red de nodos. Si agregas más, solo actualiza esta lista.
    todos_los_nodos = [('192.168.1.74', 6001), ('192.168.1.76', 6002), ('192.168.1.76', 6003)]
    mis_vecinos = [n for n in todos_los_nodos if n[1] != puerto_mio]

    iniciar_nodo(puerto_mio, carpeta_mia, mis_vecinos)