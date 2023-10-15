import socket
import threading
import json

from lmdbDB import Database
from utils import ENCODING_AND_DECODING_TYPE


def controller(replica: Database, conn: socket):
    while True:
        data = conn.recv(4120)
        msg = data.decode(ENCODING_AND_DECODING_TYPE)

        functionName = ''
        resp = None

        if msg:
            responseMsg = json.loads(msg)
            functionName = responseMsg['function']

        if functionName == 'get' or functionName == 'getAll':
            key = responseMsg['key']
            version = responseMsg['version']

            key_returned, last_value, last_version = replica.get(key, version)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'value_returned': last_value,
                    'version_returned': last_version
                }
            )
        elif functionName == 'getRange':
            start_key = responseMsg['start_key']
            end_key = responseMsg['end_key']
            start_version = responseMsg['start_version']
            end_version = responseMsg['end_version']

            replica_response_dict = \
                replica.getRange(start_key, end_key, start_version, end_version)

            resp = json.dumps(replica_response_dict)
        elif functionName == 'put' or functionName == 'putAll':
            key = responseMsg['key']
            value = responseMsg['value']

            key_returned, old_value, old_version, new_version \
                = replica.put(key, value)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'old_value': old_value,
                    'old_version': old_version,
                    'new_version': new_version
                }
            )
        elif functionName == 'del' or functionName == 'delAll':
            key = responseMsg['key']

            key_returned, last_value, last_version = replica.delete(key)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'last_value': last_value,
                    'last_version': last_version,
                }
            )
        elif functionName == 'delRange':
            start_key = responseMsg['start_key']
            end_key = responseMsg['end_key']

            replica_response_dict = replica.delRange(start_key, end_key)

            resp = json.dumps(replica_response_dict)
        elif functionName == 'trim':
            key = responseMsg['key']

            key_returned, last_value, new_version = replica.trim(key)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'last_value': last_value,
                    'new_version': new_version,
                }
            )
        else:
            resp = json.dumps({'error': 'not exist this function'})

        conn.send(resp.encode(ENCODING_AND_DECODING_TYPE))


def run() -> None:
    socketPort = 30020
    socketHost = 'localhost'
    replica = Database(
        socketPort,
        'localhost:39400',
        ['localhost:39402', 'localhost:39404']
    )

    s = socket.socket()

    s.bind((socketHost, socketPort))
    print((socketHost, socketPort))
    s.listen(30)

    while True:
        conn, addr = s.accept()
        threading.Thread(target=controller, args=(replica, conn)).start()


if __name__ == '__main__':
    run()
