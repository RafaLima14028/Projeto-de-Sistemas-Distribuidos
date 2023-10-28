from socket import socket
import threading
import json

from lmdbDB import Database
from utils import ENCODING_AND_DECODING_TYPE


def controller(replica: Database, conn: socket) -> None:
    while True:
        data = conn.recv(4120)
        msg = data.decode(ENCODING_AND_DECODING_TYPE)

        function_name = ''
        resp = None
        response_msg = None

        if msg:
            response_msg = json.loads(msg)
            function_name = response_msg['function']

        if function_name == 'get' or function_name == 'getAll':
            key = response_msg['key']
            version = response_msg['version']

            key_returned, last_value, last_version = replica.get(key, version)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'value_returned': last_value,
                    'version_returned': last_version
                }
            )
        elif function_name == 'getRange':
            start_key = response_msg['start_key']
            end_key = response_msg['end_key']
            start_version = response_msg['start_version']
            end_version = response_msg['end_version']

            replica_response_dict = replica.getRange(start_key, end_key, start_version, end_version)

            resp = json.dumps(replica_response_dict)
        elif function_name == 'put' or function_name == 'putAll':
            key = response_msg['key']
            value = response_msg['value']

            key_returned, old_value, old_version, new_version = replica.put(key, value)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'old_value': old_value,
                    'old_version': old_version,
                    'new_version': new_version
                }
            )
        elif function_name == 'del' or function_name == 'delAll':
            key = response_msg['key']

            key_returned, last_value, last_version = replica.delete(key)

            resp = json.dumps(
                {
                    'key': key_returned,
                    'last_value': last_value,
                    'last_version': last_version,
                }
            )
        elif function_name == 'delRange':
            start_key = response_msg['start_key']
            end_key = response_msg['end_key']

            replica_response_dict = replica.delRange(start_key, end_key)

            resp = json.dumps(replica_response_dict)
        elif function_name == 'trim':
            key = response_msg['key']

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
    primary_node = 'localhost:39400'
    seconde_node = 'localhost:39401'
    third_node = 'localhost:39402'

    replica1 = Database(primary_node, [seconde_node, third_node])
    replica2 = Database(seconde_node, [primary_node, third_node])
    replica3 = Database(third_node, [primary_node, seconde_node])

    skt = socket()

    socket_port = 30020
    socket_host = 'localhost'

    skt.bind((socket_host, socket_port))
    print((socket_host, socket_port))
    skt.listen(30)

    while True:
        conn, addr = skt.accept()
        threading.Thread(target=controller, args=(replica1, conn)).start()


if __name__ == '__main__':
    run()
