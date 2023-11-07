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


def init_bd(bd: str):
    match bd:
        case 'bd1':
            port = 44001
            socket_port = SERVER_DB_SOCKET_PORT
            other_nodes = [f'{SERVER_DB_ADDRESS}:{44002}', f'{SERVER_DB_ADDRESS}:{44003}']
        case 'bd2':
            port = 44002
            socket_port = SERVER_DB_SOCKET_PORT + 1
            other_nodes = [f'{SERVER_DB_ADDRESS}:{44001}', f'{SERVER_DB_ADDRESS}:{44003}']
        case 'bd3':
            port = 44003
            socket_port = SERVER_DB_SOCKET_PORT + 2
            other_nodes = [f'{SERVER_DB_ADDRESS}:{44001}', f'{SERVER_DB_ADDRESS}:{44002}']
        case _:
            print(f'Invalid argument: {bd}')
            exit(1)

    bd_node = f'{SERVER_DB_ADDRESS}:{port}'
    replica = Database(bd_node, other_nodes, bd)

    skt = socket.socket()
    skt.bind((SERVER_DB_ADDRESS, socket_port))
    skt.listen(30)
    print()
    print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
    print('Ready...')

    return replica, skt


def run(db1: bool = False, db2: bool = False, db3: bool = False) -> None:
    if not db1 and not db2 and not db3:
        print(f'Error: no Database specified')
        return

    replica_db1 = None
    replica_db2 = None
    replica_db3 = None

    skt1 = None
    skt2 = None
    skt3 = None

    # bd = None

    if db1:
        replica_db1, skt1 = init_bd('bd1')
    if db2:
        replica_db2, skt2 = init_bd('bd2')
    if db3:
        replica_db3, skt3 = init_bd('bd3')

    while True:
        if db1:
            conn, addr = skt1.accept()
            threading.Thread(target=controller, args=(replica_db1, conn, addr)).start()
        if db2:
            conn, addr = skt2.accept()
            threading.Thread(target=controller, args=(replica_db2, conn, addr)).start()
        if db3:
            conn, addr = skt3.accept()
            threading.Thread(target=controller, args=(replica_db3, conn, addr)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-db1', action='store_true', help='Opção -db1')
    parser.add_argument('-db2', action='store_true', help='Opção -db2')
    parser.add_argument('-db3', action='store_true', help='Opção -db3')

    args = parser.parse_args()

    _db1 = False
    _db2 = False
    _db3 = False

    if args.db1:
        _db1 = True
    if args.db2:
        _db2 = True
    if args.db3:
        _db3 = True

    run(_db1, _db2, _db3)
