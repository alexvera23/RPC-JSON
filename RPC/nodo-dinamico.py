import socket
import json
import base64
import os
import threading
import sys
import random

# --- ESTADO GLOBAL DEL NODO ---
IP_LOCAL = '172.26.166.127' # Cambia esto a tu IP real
PUERTO_LOCAL = 0
CAPACIDAD_BYTES = 0
VECINOS = set()
LOCK_VECINOS = threading.Lock()

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

def obtener_tamano_carpeta(carpeta):
    total = 0
    if not os.path.exists(carpeta): return 0
    for archivo in os.listdir(carpeta):
        ruta = os.path.join(carpeta, archivo)
        if os.path.isfile(ruta):
            total += os.path.getsize(ruta)
    return total

def notificar_a_vecinos_del_nuevo(ip_nuevo, puerto_nuevo):
    payload = {
        "jsonrpc": "2.0",
        "method": "join_network",
        "params": {"ip": ip_nuevo, "port": puerto_nuevo, "is_broadcast": True},
        "id": 1
    }
    
    with LOCK_VECINOS:
        lista_actual = list(VECINOS)
        
    for ip_v, puerto_v in lista_actual:
        if (ip_v, puerto_v) == (ip_nuevo, puerto_nuevo): continue
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((ip_v, puerto_v))
                enviar_mensaje(s, payload)
        except:
            with LOCK_VECINOS: VECINOS.discard((ip_v, puerto_v))

def reenviar_a_vecino(payload_dict):
    with LOCK_VECINOS:
        lista_vecinos = list(VECINOS)
        
    if not lista_vecinos:
        return {"jsonrpc": "2.0", "error": {"code": -32000, "message": "No tengo vecinos a quien pedir ayuda."}}
        
    random.shuffle(lista_vecinos)
    
    for ip_vecino, puerto_vecino in lista_vecinos:
        try:
            print(f"      [-> NODO {ip_vecino}:{puerto_vecino}] Rebotando archivo pesado...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_vecino:
                # --- SOLUCIÓN DE REBOTE PARA ARCHIVOS GRANDES ---
                s_vecino.settimeout(3) # Fase 1: Comprobar que el vecino está vivo
                s_vecino.connect((ip_vecino, puerto_vecino))
                
                s_vecino.settimeout(300) # Fase 2: 5 Minutos para transmitirle el peso
                enviar_mensaje(s_vecino, payload_dict)
                
                data = recibir_mensaje(s_vecino)
                if data:
                    return json.loads(data.decode('utf-8'))
        except Exception as e:
            print(f"      [x] Falló el vecino {ip_vecino}:{puerto_vecino} ({e})")
            with LOCK_VECINOS: VECINOS.discard((ip_vecino, puerto_vecino))
            continue
            
    return {"jsonrpc": "2.0", "error": {"code": -32001, "message": "Todos los vecinos fallaron en recibir el archivo."}}

def manejar_cliente(conn, addr, carpeta_raiz):
    try:
        data = recibir_mensaje(conn)
        if not data: return
        
        request = json.loads(data.decode('utf-8'))
        metodo = request.get('method')
        params = request.get('params')
        id_req = request.get('id')
        response = {"jsonrpc": "2.0", "id": id_req}

        if metodo == 'upload':
            ttl = params.get('ttl', 3)
            nombre = params['filename']
            datos_binarios = base64.b64decode(params['content'])
            peso_archivo = len(datos_binarios)
            espacio_ocupado = obtener_tamano_carpeta(carpeta_raiz)
            
            if espacio_ocupado + peso_archivo <= CAPACIDAD_BYTES:
                print(f"[*] Guardando '{nombre}' | Ocupado: {(espacio_ocupado+peso_archivo)/1024/1024:.2f} MB / {CAPACIDAD_BYTES/1024/1024:.0f} MB")
                ruta_final = os.path.join(carpeta_raiz, nombre)
                with open(ruta_final, 'wb') as f:
                    f.write(datos_binarios)
                response['result'] = f"Guardado en Nodo {IP_LOCAL}:{PUERTO_LOCAL}"
            else:
                if ttl > 1:
                    print(f"[!] Capacidad Llena. Rebotando archivo (TTL: {ttl-1})")
                    params['ttl'] = ttl - 1
                    request['params'] = params
                    response = reenviar_a_vecino(request)
                else:
                    response['error'] = {"code": -32002, "message": "Red llena. No hay capacidad."}

        elif metodo == 'join_network':
            ip_n = params['ip']
            puerto_n = params['port']
            es_broadcast = params.get('is_broadcast', False)
            
            with LOCK_VECINOS:
                if (ip_n, puerto_n) != (IP_LOCAL, PUERTO_LOCAL):
                    VECINOS.add((ip_n, puerto_n))
            
            if not es_broadcast:
                print(f"[RED] Nuevo nodo unido: {ip_n}:{puerto_n}. Avisando al resto...")
                nodos_conocidos = list(VECINOS) + [(IP_LOCAL, PUERTO_LOCAL)]
                response['result'] = {"known_nodes": nodos_conocidos}
                threading.Thread(target=notificar_a_vecinos_del_nuevo, args=(ip_n, puerto_n)).start()
            else:
                response['result'] = "OK"

        elif metodo == 'get_nodes':
            with LOCK_VECINOS:
                todos = list(VECINOS) + [(IP_LOCAL, PUERTO_LOCAL)]
            response['result'] = {"nodes": todos}

        else:
            response['error'] = {"code": -32601, "message": "Método desconocido"}

        enviar_mensaje(conn, response)
            
    except Exception as e:
        print(f"Error interno: {e}")
    finally:
        conn.close()

def conectar_a_red(nodos_semilla):
    payload = {
        "jsonrpc": "2.0",
        "method": "join_network",
        "params": {"ip": IP_LOCAL, "port": PUERTO_LOCAL, "is_broadcast": False},
        "id": 1
    }
    
    conectado = False
    for boot_ip, boot_puerto in nodos_semilla:
        if (boot_ip, boot_puerto) == (IP_LOCAL, PUERTO_LOCAL): continue 
        
        try:
            print(f"[*] Intentando unirse vía nodo semilla {boot_ip}:{boot_puerto}...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((boot_ip, boot_puerto))
                enviar_mensaje(s, payload)
                
                data = recibir_mensaje(s)
                if data:
                    res = json.loads(data.decode('utf-8'))
                    if 'result' in res and 'known_nodes' in res['result']:
                        nodos_red = res['result']['known_nodes']
                        with LOCK_VECINOS:
                            for n_ip, n_puerto in nodos_red:
                                if (n_ip, n_puerto) != (IP_LOCAL, PUERTO_LOCAL):
                                    VECINOS.add((tuple((n_ip, n_puerto))))
                        print(f"[*] Integración exitosa. Descubrí {len(VECINOS)} vecinos en la red.")
                        conectado = True
                        break
        except Exception as e:
            print(f"    -> Falló conexión con {boot_ip}:{boot_puerto}.")
            
    if not conectado:
        print("[!] Iniciando como red aislada (Nodo Génesis).")

def iniciar_nodo():
    carpeta = f"datos_nodo_{PUERTO_LOCAL}"
    if not os.path.exists(carpeta): os.makedirs(carpeta)
        
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', PUERTO_LOCAL))
    s.listen()
    print(f"=================================================")
    print(f"NODO P2P INICIADO | MI IP: {IP_LOCAL}:{PUERTO_LOCAL}")
    print(f"CAPACIDAD MÁXIMA  : {CAPACIDAD_BYTES / 1024 / 1024:.2f} MB")
    print(f"=================================================")
    
    while True:
        conn, addr = s.accept()
        threading.Thread(target=manejar_cliente, args=(conn, addr, carpeta)).start()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python nodo_dinamico.py <mi_puerto> <capacidad_MB> [<ip_semilla:puerto_semilla>]")
        sys.exit()

    PUERTO_LOCAL = int(sys.argv[1])
    CAPACIDAD_BYTES = float(sys.argv[2]) * 1024 * 1024 
    
    hilo_servidor = threading.Thread(target=iniciar_nodo, daemon=True)
    hilo_servidor.start()

    nodos_semilla = [["172.26.166.127", 6001],
        ["172.26.166.127", 6002],
        ["192.168.1.76", 6003]]
    
    if len(sys.argv) == 4:
        semilla_str = sys.argv[3]
        if ":" in semilla_str:
            ip_sem, puerto_sem = semilla_str.split(":")
            nodos_semilla.insert(0, (ip_sem, int(puerto_sem)))
        else:
            nodos_semilla = [(IP_LOCAL, int(semilla_str))]

    conectar_a_red(nodos_semilla)

    hilo_servidor.join()