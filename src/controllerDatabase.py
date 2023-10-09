from lmdbDB import Database
import socket
import threading
import json


def controller(replica: Database, conn: socket):
    while True:
        data = conn.recv(4120)
        msg = data.decode()

        functionName = ''
        key = ''
        value = ''
        version = -1
        resp = None

        if msg:
            responseMsg = json.loads(msg)
            functionName = responseMsg['function']
            key = responseMsg['key']
            value = responseMsg['value']
            version = responseMsg['version']

        if functionName == 'get':
            response = replica.get(key, version)

            if response:
                response = str(response).replace('\'', '"')
                response = json.loads(response)

            resp = json.dumps({'data': response})
        elif functionName == 'insert/update':
            replica.put(key, value)
            resp = json.dumps({'message': "OK"})
        elif functionName == 'delete':
            replica.delete(key)
            resp = json.dumps({'message': "OK"})
        elif functionName == 'trim':
            replica.trim(key)
            # add response

        conn.send(resp.encode())


def run() -> None:
    socketPort = 30020
    replica = Database(socketPort, 'localhost:39400', ['localhost:39402', 'localhost:39404'])

    s = socket.socket()
    host = socket.gethostname()

    s.bind(('localhost', socketPort))
    print(('localhost', socketPort))
    s.listen()

    print('Dispositivo aceito')

    while True:
        conn, addr = s.accept()
        threading.Thread(target=controller, args=(replica, conn)).start()


if __name__ == '__main__':
    run()
