import argparse
import socket
import threading
import json
import os
from time import sleep

from src.lmdbDB import Database
from src.utils import (ENCODING_AND_DECODING_TYPE, SERVER_DB_ADDRESS,
                       SERVER_DB_SOCKET_PORT, DATABASE_PORTS_FILE)


def controller(replica: Database, conn: socket, addr: tuple) -> None:
    print()
    print(f'Connected with the socket: {addr}')

    while True:
        try:
            data = conn.recv(4120)
        except ConnectionResetError:
            print(f'Connection terminated with the {addr}')
            conn.close()
            break
        except ConnectionAbortedError:
            print(f'Connection terminated with the {addr}')
            conn.close()
            break

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
            resp = json.dumps(
                {
                    'error':
                        'not exist this function'
                }
            )

        conn.send(resp.encode(ENCODING_AND_DECODING_TYPE))


def run(db1: bool = False, db2: bool = False, db3: bool = False) -> None:
    if not db1 and not db2 and not db3:
        print(f'Error: No Database Passed')
        return

    port = 44000
    replica_db1 = None
    replica_db2 = None
    replica_db3 = None

    bd = None

    # TODO: ARRUMAR DAQUI PARA BAIXO, PARA ACEITAR VÁRIOS DBS NO MESMO ARQUIVO

    match bd:
        case 'bd1':
            port = port + 1
            socket_port = SERVER_DB_SOCKET_PORT

            bd_node = f'{SERVER_DB_ADDRESS}:{port}'
            otherNodes = [f'{SERVER_DB_ADDRESS}:{port + 1}', f'{SERVER_DB_ADDRESS}:{port + 2}']
            replica = Database(bd_node, otherNodes, bd)

            pass
        case 'bd2':
            port = port + 2
            socket_port = SERVER_DB_SOCKET_PORT + 1

            bd_node = f'{SERVER_DB_ADDRESS}:{port}'
            otherNodes = [f'{SERVER_DB_ADDRESS}:{port - 1}', f'{SERVER_DB_ADDRESS}:{port + 1}']
            replica = Database(bd_node, otherNodes, bd)

            pass
        case 'bd3':
            port = port + 3
            socket_port = SERVER_DB_SOCKET_PORT + 2

            bd_node = f'{SERVER_DB_ADDRESS}:{port}'
            otherNodes = [f'{SERVER_DB_ADDRESS}:{port - 2}', f'{SERVER_DB_ADDRESS}:{port - 1}']
            replica = Database(bd_node, otherNodes, bd)

            pass
        case _:
            print(f'Invalid argument: {bd}')
            exit(1)

    sleep(5)

    skt = socket.socket()

    skt.bind((SERVER_DB_ADDRESS, socket_port))
    print()
    print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
    print('Ready...')
    skt.listen(30)

    while True:
        conn, addr = skt.accept()
        threading.Thread(target=controller, args=(replica, conn, addr)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-db1', action='store_true', help='Opção -db1')
    parser.add_argument('-db2', action='store_true', help='Opção -db2')
    parser.add_argument('-db3', action='store_true', help='Opção -db3')

    args = parser.parse_args()

    db1 = False
    db2 = False
    db3 = False

    if args.db1:
        db1 = True
    if args.db2:
        db2 = True
    if args.db3:
        db3 = True

    run(db1, db2, db3)
