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
        self.peers = []
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
        for peer in self.peers:
            if peer != client:
                peer.send(pickle.dumps(request))

    def start_client(self, peer_host, peer_port):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((peer_host, peer_port))
        self.peers.append(client)
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

    def request_file(self, filename):
        request = {'type': 'bloom_check', 'filename': filename}
        print(f"[Nodo {self.port}] Requesting file: {filename}")
        for peer in self.peers:
            peer.send(pickle.dumps(request))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python P6.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    node = P2PNode('localhost', port)
    threading.Thread(target=node.start_server).start()

    if port == 5000:
        # Nodo 1
        node.start_client('localhost', 5001)
        node.request_file('somefile.txt')
    elif port == 5001:
        # Nodo 2
        node.start_client('localhost', 5002)
    elif port == 5002:
        # Nodo 3
        pass
