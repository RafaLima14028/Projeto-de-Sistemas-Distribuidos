import argparse
import socket
import threading
import json
import os

from src.lmdbDB import Database
from src.utils import ENCODING_AND_DECODING_TYPE, SERVER_DB_ADDRESS, SERVER_DB_SOCKET_PORT, DATABASE_PORTS_FILE


def write_port(port: int) -> None:
    if port not in read_ports():
        with open(DATABASE_PORTS_FILE, 'a') as file:
            file.write(str(port) + '\n')


def read_ports() -> list:
    ports = []

    if os.path.exists(DATABASE_PORTS_FILE):
        with open(DATABASE_PORTS_FILE, 'r') as file:
            for line in file:
                port = int(line.strip())
                ports.append(port)

    return ports


def remove_port(port: int) -> None:
    if os.path.exists(DATABASE_PORTS_FILE):
        with open(DATABASE_PORTS_FILE, 'w') as file:
            for line in file:
                port_line = int(line.strip())
                if port == port_line:
                    continue

                file.write(line)


def connect_other_ports(replica: Database) -> None:
    if os.path.exists(DATABASE_PORTS_FILE):
        with open(DATABASE_PORTS_FILE, 'r') as file:
            for line in file:
                port = int(line.strip())

                if port != replica.get_port():
                    replica.addNodeToCluster(f'{SERVER_DB_ADDRESS}:{port}')


def manages_ports(ports_in_txt: [int], replica: Database) -> [int]:
    ports_in_txt_tmp = read_ports()

    # Check if any ports have been dropped
    for port_tmp in ports_in_txt_tmp:
        if port_tmp != replica.get_port():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)

            try:
                sock.connect((SERVER_DB_ADDRESS, port_tmp))  # Connection Successful
            except socket.error:
                remove_port(port_tmp)
            finally:
                sock.close()

    # Scans new port and connects
    for port_tmp in ports_in_txt_tmp:
        if port_tmp not in ports_in_txt:
            replica.addNodeToCluster(f'{SERVER_DB_ADDRESS}:{port_tmp}')
            ports_in_txt = ports_in_txt_tmp

    return ports_in_txt_tmp


def controller(replica: Database, conn: socket) -> None:
    ports_in_txt = read_ports()

    while True:
        data = conn.recv(4120)
        msg = data.decode(ENCODING_AND_DECODING_TYPE)

        function_name = ''
        resp = None
        response_msg = None

        ports_in_txt = manages_ports(ports_in_txt, replica)

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


def run(bd: str, port: int = None) -> None:
    if port is None:
        port = 39400
    else:
        port = int(port)

    match bd:
        case 'bd1':
            port = port + 1
            socket_port = SERVER_DB_SOCKET_PORT
            write_port(port)
            pass
        case 'bd2':
            port = port + 2
            socket_port = SERVER_DB_SOCKET_PORT + 1
            write_port(port)
            pass
        case 'bd3':
            port = port + 3
            socket_port = SERVER_DB_SOCKET_PORT + 2
            write_port(port)
            pass
        case _:
            print(f'Invalid argument: {bd}')
            exit(1)

    bd_node = f'{SERVER_DB_ADDRESS}:{port}'
    replica = Database(bd_node, bd)

    print()
    ports_in_txt = []
    ports_in_txt = manages_ports(ports_in_txt, replica)
    connect_other_ports(replica)

    skt = socket.socket()

    skt.bind((SERVER_DB_ADDRESS, socket_port))
    print()
    print(f'Database socket initialized at: {(SERVER_DB_ADDRESS, socket_port)}')
    skt.listen(30)

    while True:
        conn, addr = skt.accept()
        threading.Thread(target=controller, args=(replica, conn)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('bd')
    parser.add_argument('--port')

    args = parser.parse_args()

    run(args.bd, args.port)
