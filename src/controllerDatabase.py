import argparse
import socket
import threading
import json
import os
from time import sleep

from src.lmdbDB import Database
from src.utils import (ENCODING_AND_DECODING_TYPE, SERVER_DB_ADDRESS,
                       SERVER_DB_SOCKET_PORT, DATABASE_PORTS_FILE)


def write_port(port_db: int, port_socket: int) -> None:
    if port_db not in read_ports_db() and port_socket not in read_ports_socket():
        with open(DATABASE_PORTS_FILE, 'a') as file:
            file.write(str(port_db) + '-' + str(port_socket) + '\n')


def read_ports_db() -> list:
    ports = []

    if os.path.exists(DATABASE_PORTS_FILE):
        with open(DATABASE_PORTS_FILE, 'r') as file:
            for line in file:
                port = int(line.split('-')[0])
                ports.append(port)

    return ports


def read_ports_socket() -> list:
    ports = []

    if os.path.exists(DATABASE_PORTS_FILE):
        with open(DATABASE_PORTS_FILE, 'r') as file:
            for line in file:
                port = int(line.split('-')[1].split('\n')[0])
                ports.append(port)

    return ports


def remove_port(port_db: int, port_socket: int) -> None:
    if os.path.exists(DATABASE_PORTS_FILE):
        lines = None

        with open(DATABASE_PORTS_FILE, 'r') as file:
            lines = file.readlines()

        with open(DATABASE_PORTS_FILE, 'w') as file:
            for line in lines:
                line_split = line.split('-')
                port_line_db = int(line_split[0])
                port_line_socket = int(line_split[1].split('\n')[0])

                if port_line_socket != port_socket and port_line_db != port_db:
                    file.write(line)


def manages_ports(replica: Database) -> None:
    error_in_port = []

    # Check if any ports have been dropped
    ports_range_socket = read_ports_socket()
    ports_range_dbs = read_ports_db()

    for port_socket, port_db in zip(ports_range_socket, ports_range_dbs):
        if port_db != replica.get_port():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)

            try:
                sock.connect((SERVER_DB_ADDRESS, port_socket))
            except socket.error:
                try:
                    replica.removeNodeFromCluster(f'{SERVER_DB_ADDRESS}:{port_db}')
                except Exception:
                    pass

                remove_port(port_db, port_socket)
                error_in_port.append(port_db)
                print(f'The node {SERVER_DB_ADDRESS}:{port_socket} connection dropped')
            finally:
                sock.close()

    ports_in_txt = read_ports_db()

    # Scans new port and connects
    for port_socket in ports_in_txt:
        if port_socket != replica.get_port():
            if port_socket not in error_in_port:
                replica.addNodeToCluster(f'{SERVER_DB_ADDRESS}:{port_socket}')
                sleep(2)
                print(f'Connected with {SERVER_DB_ADDRESS}:{port_socket}')
                ports_in_txt = ports_in_txt

    sleep(5)


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


def run(bd: str, port: int = None) -> None:
    if port is None:
        port = 44000
    else:
        if port in [SERVER_DB_SOCKET_PORT, SERVER_DB_SOCKET_PORT + 1, SERVER_DB_SOCKET_PORT + 2]:
            port = 44000
        else:
            port = int(port)

    match bd:
        case 'bd1':
            port = port + 1
            socket_port = SERVER_DB_SOCKET_PORT
            write_port(port, socket_port)
            pass
        case 'bd2':
            port = port + 2
            socket_port = SERVER_DB_SOCKET_PORT + 1
            write_port(port, socket_port)
            pass
        case 'bd3':
            port = port + 3
            socket_port = SERVER_DB_SOCKET_PORT + 2
            write_port(port, socket_port)
            pass
        case _:
            print(f'Invalid argument: {bd}')
            exit(1)

    bd_node = f'{SERVER_DB_ADDRESS}:{port}'
    replica = Database(bd_node, bd)

    sleep(5)
    threading.Thread(target=manages_ports, args=(replica,)).start()

    skt = socket.socket()

    skt.bind((SERVER_DB_ADDRESS, socket_port))
    print()
    print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
    skt.listen(30)

    while True:
        conn, addr = skt.accept()
        threading.Thread(target=controller, args=(replica, conn, addr)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('bd')
    parser.add_argument('--port')

    args = parser.parse_args()

    run(args.bd, args.port)
