import lmdb
from pysyncobj import SyncObj
import struct
import pickle
from time import time


class Database(SyncObj):
    def __init__(self, port, primary, secundary):
        super(Database, self).__init__(primary, secundary)
        self.path_database = f'my_db/{port}/'

    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value_bytes, old_version = self.get(key)

        db = lmdb.open(self.path_database, sync=True, writemap=True)

        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')

        new_version = int(time())
        new_version_bytes = struct.pack('>Q', new_version)

        with db.begin(write=True) as txn:
            try:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)
                else:
                    existing_values = []

                existing_values.append((new_version_bytes, value_bytes))
                txn.put(key_bytes, pickle.dumps(existing_values))
            except lmdb.Error as e:
                raise Exception(f'A database insertion failure occurred: {str(e)}')

        return key, old_value_bytes, old_version, new_version

    def get_all_values_key(self, key: str) -> [(str, int)]:
        db = lmdb.open(self.path_database, sync=True, writemap=True)

        key_bytes = key.encode('utf-8')
        values = []

        with db.begin() as txn:
            try:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)

                    for version_bytes, value_bytes in existing_values:
                        version = struct.unpack('>Q', version_bytes)[0]
                        values.append((value_bytes.decode('utf-8'), version))
            except lmdb.Error as e:
                raise Exception(f'Failed to fetch data from the database: {str(e)}')

        return values

    def get(self, key: str, version: int = -1) -> (str, str, int):
        values = self.get_all_values_key(key)

        if not values:
            return '', '', -1

        max_tuple = (None, None, None)

        if version > 0:
            max_version = -1

            for t in values:
                value_tuple = t[0]
                version_tuple = t[1]

                if version >= version_tuple > max_version:
                    max_version = version_tuple
                    max_tuple = (key, value_tuple, version_tuple)
        else:
            max_version = -1

            for t in values:
                value_tuple = t[0]
                version_tuple = t[1]

                if version_tuple > max_version:
                    max_version = version_tuple
                    max_tuple = (key, value_tuple, version_tuple)

        return max_tuple

    def delete(self, key: str) -> (str, str, int):
        key_bytes = key.encode('utf-8')

        _, last_value, last_version = self.get(key)

        db = lmdb.open(self.path_database, sync=True, writemap=True)

        with db.begin(write=True) as txn:
            try:
                txn.delete(key_bytes)
            except lmdb.Error as e:
                raise Exception(f'Database deletion failed: {str(e)}')

        return key, last_value, last_version

    def trim(self, key: str) -> (str, str, int):
        key_bytes = key.encode('utf-8')

        _, last_value, last_version = self.delete(key)

        value_bytes = last_value.encode('utf-8')

        new_version = last_version
        new_version_bytes = struct.pack('>Q', new_version)

        db = lmdb.open(self.path_database, sync=True, writemap=True)

        with db.begin(write=True) as txn:
            try:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)
                else:
                    existing_values = []

                existing_values.append((new_version_bytes, value_bytes))
                txn.put(key_bytes, pickle.dumps(existing_values))
            except lmdb.Error as e:
                raise Exception(f'A database insertion failure occurred: {str(e)}')

        return key, last_value, new_version


if __name__ == '__main__':
    server = Database(
        '50055',
        'localhost:50056',
        ['localhost:50057', 'localhost:50058']
    )

    print(server.put('Rafael', 'Alves'))
    print(server.get('Rafael'))
