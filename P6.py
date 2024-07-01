import socket
import threading
import pickle
import os
import sys
from bitarray import bitarray
import mmh3

class BloomFilter:
    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    def add(self, item):
        for i in range(self.hash_count):
            index = mmh3.hash(item, i) % self.size
            self.bit_array[index] = 1

    def check(self, item):
        for i in range(self.hash_count):
            index = mmh3.hash(item, i) % self.size
            if self.bit_array[index] == 0:
                return False
        return True

class P2PNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.peers = {}
        self.bloom_filter = BloomFilter(5000, 7)
        self.index_files()

    def index_files(self):
        for filename in os.listdir('.'):
            if os.path.isfile(filename):
                self.bloom_filter.add(filename)
        print(f"[Nodo {self.port}] Archivos indexados: {os.listdir('.')}")

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"[Nodo {self.port}] Listening on {self.host}:{self.port}")
        while True:
            client, address = server.accept()
            print(f"[Nodo {self.port}] Connected to {address}")
            threading.Thread(target=self.handle_client, args=(client,)).start()

    def handle_client(self, client):
        while True:
            try:
                data = client.recv(4096)
                if data:
                    request = pickle.loads(data)
                    print(f"[Nodo {self.port}] Received request: {request}")
                    self.process_request(request, client)
            except:
                client.close()
                print(f"[Nodo {self.port}] Connection closed")
                return
            
    def process_request(self, request, client):
        if request['type'] == 'bloom_check':
            filename = request['filename']
            present = self.bloom_filter.check(filename)
            response = {'type': 'bloom_check_response', 'filename': filename, 'present': present}
            client.send(pickle.dumps(response))
            print(f"[Nodo {self.port}] Bloom check for {filename}: {'Present' if present else 'Not present'}")
        elif request['type'] == 'file_request':
            filename = request['filename']
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    data = f.read()
                response = {'type': 'file_data', 'filename': filename, 'data': data}
                client.send(pickle.dumps(response))
                print(f"[Nodo {self.port}] Sent file {filename}")
            else:
                print(f"[Nodo {self.port}] No se encontró el archivo {filename} en este nodo.")
        elif request['type'] == 'file_data':
            filename = request['filename']
            data = request['data']
            with open(f"received_{filename}", 'wb') as f:
                f.write(data)
            print(f"[Nodo {self.port}] File {filename} received and saved as received_{filename}")
        elif request['type'] == 'bloom_check_response':
            if request['present']:
                file_request = {'type': 'file_request', 'filename': request['filename']}
                client.send(pickle.dumps(file_request))
                print(f"[Nodo {self.port}] File {request['filename']} present. Requesting file.")
            else:
                self.forward_request(request, client)


    def forward_request(self, request, client):
        print(f"[Nodo {self.port}] Forwarding request: {request}")
        found = False
        
        # Intentar enviar la solicitud a todos los pares disponibles
        for peer_host, peer_port in self.peers.keys():
            if (peer_host, peer_port) != client.getpeername():
                peer_socket = self.peers[(peer_host, peer_port)]
                try:
                    peer_socket.send(pickle.dumps(request))
                    found = True
                    break
                except Exception as e:
                    print(f"[Nodo {self.port}] Error al enviar solicitud a {peer_host}:{peer_port}: {e}")
                    continue
        
        # Si no se encontró en los nodos directos, buscar en otros nodos
        if not found:
            print(f"[Nodo {self.port}] No se encontró el archivo en los nodos directos. Buscando en otros nodos...")
            for peer_host, peer_port in self.peers.keys():
                if (peer_host, peer_port) != client.getpeername():
                    peer_socket = self.peers[(peer_host, peer_port)]
                    try:
                        peer_socket.send(pickle.dumps(request))
                        found = True
                        break
                    except Exception as e:
                        print(f"[Nodo {self.port}] Error al enviar solicitud a {peer_host}:{peer_port}: {e}")
                        continue
        
        # Si aún no se encontró, imprimir mensaje adecuado
        if not found:
            print(f"[Nodo {self.port}] El archivo solicitado no se encontró en la red.")



    def start_client(self, peer_host, peer_port):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((peer_host, peer_port))
        self.peers[(peer_host, peer_port)] = client
        threading.Thread(target=self.listen_to_peer, args=(client,)).start()
        print(f"[Nodo {self.port}] Connected to peer at {peer_host}:{peer_port}")

    def listen_to_peer(self, peer):
        while True:
            try:
                data = peer.recv(4096)
                if data:
                    response = pickle.loads(data)
                    print(f"[Nodo {self.port}] Received response: {response}")
                    self.process_request(response, peer)
            except:
                peer.close()
                print(f"[Nodo {self.port}] Peer connection closed")
                return

    def request_file(self, filename, peer_host, peer_port):
        request = {'type': 'bloom_check', 'filename': filename}
        print(f"[Nodo {self.port}] Requesting file: {filename} from {peer_host}:{peer_port}")
        peer = self.peers.get((peer_host, peer_port))
        if peer:
            peer.send(pickle.dumps(request))
        else:
            print(f"[Nodo {self.port}] No connection to {peer_host}:{peer_port}")

    def list_files(self):
        print(f"[Nodo {self.port}] Archivos indexados: {os.listdir('.')}")

    def list_peers(self):
        print(f"[Nodo {self.port}] Conexiones a pares: {list(self.peers.keys())}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P6.py <port> [<peer_host> <peer_port>]")
        sys.exit(1)

    port = int(sys.argv[1])
    node = P2PNode('localhost', port)
    threading.Thread(target=node.start_server).start()

    if len(sys.argv) == 4:
        peer_host = sys.argv[2]
        peer_port = int(sys.argv[3])
        node.start_client(peer_host, peer_port)

    while True:
        command = input(f"[Nodo {port}] Enter command (list_peers, list_files, request_file <filename> <peer_host> <peer_port>, exit): ")
        if command == "list_peers":
            node.list_peers()
        elif command == "list_files":
            node.list_files()
        elif command.startswith("request_file"):
            _, filename, peer_host, peer_port = command.split()
            peer_port = int(peer_port)
            node.request_file(filename, peer_host, peer_port)
        elif command == "exit":
            break
