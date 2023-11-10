import argparse
import socket
import threading
import json
from time import sleep

from lmdbDB import Database
from utils import ENCODING_AND_DECODING_TYPE, SERVER_DB_ADDRESS, SERVER_DB_SOCKET_PORT


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


def init_bd(bd: str) -> (Database, socket):
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
    print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
    sleep(2)
    print('Ready...')
    print()

    return replica, skt


def run(db1: bool = False, db2: bool = False, db3: bool = False) -> None:
    if not db1 and not db2 and not db3:
        print(f'Error: no Database specified')
        return
    elif db1 and not db2 and not db3:
        port = 44001
        bd_node = f'{SERVER_DB_ADDRESS}:{port}'
        socket_port = SERVER_DB_SOCKET_PORT

        replica_db1 = Database(bd_node, [], 'bd1')

        skt1 = socket.socket()
        skt1.bind((SERVER_DB_ADDRESS, socket_port))
        skt1.listen(30)
        print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
        sleep(2)
        print('Ready...')
        print()

        while True:
            conn, addr = skt1.accept()
            threading.Thread(target=controller(replica_db1, conn, addr)).start()
    elif not db1 and db2 and not db3:
        port = 44002
        bd_node = f'{SERVER_DB_ADDRESS}:{port}'
        socket_port = SERVER_DB_SOCKET_PORT

        replica_db2 = Database(bd_node, [], 'bd2')

        skt2 = socket.socket()
        skt2.bind((SERVER_DB_ADDRESS, socket_port))
        skt2.listen(30)
        print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
        sleep(2)
        print('Ready...')
        print()

        while True:
            conn, addr = skt2.accept()
            threading.Thread(target=controller(replica_db2, conn, addr)).start()
    elif not db1 and not db2 and db3:
        port = 44003
        bd_node = f'{SERVER_DB_ADDRESS}:{port}'
        socket_port = SERVER_DB_SOCKET_PORT

        replica_db3 = Database(bd_node, [], 'bd3')

        skt3 = socket.socket()
        skt3.bind((SERVER_DB_ADDRESS, socket_port))
        skt3.listen(30)
        print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
        sleep(2)
        print('Ready...')
        print()

        while True:
            conn, addr = skt3.accept()
            threading.Thread(target=controller(replica_db3, conn, addr)).start()
    else:
        replica_db1 = None
        replica_db2 = None
        replica_db3 = None

        skt1 = None
        skt2 = None
        skt3 = None

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
                continue
            if db2:
                conn, addr = skt2.accept()
                threading.Thread(target=controller, args=(replica_db2, conn, addr)).start()
                continue
            if db3:
                conn, addr = skt3.accept()
                threading.Thread(target=controller, args=(replica_db3, conn, addr)).start()
                continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-bd1', action='store_true', help='Opção -bd1')
    parser.add_argument('-bd2', action='store_true', help='Opção -bd2')
    parser.add_argument('-bd3', action='store_true', help='Opção -bd3')

    args = parser.parse_args()

    _bd1 = False
    _bd2 = False
    _bd3 = False

    if args.bd1:
        _bd1 = True
    if args.bd2:
        _bd2 = True
    if args.bd3:
        _bd3 = True

    run(_bd1, _bd2, _bd3)
