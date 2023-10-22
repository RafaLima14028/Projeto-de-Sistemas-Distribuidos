from time import time, sleep
import socket

from handlesJSON import HandlesJsonCache


class ManipulateDictionary:
    __dictionary = None

    def __init__(self):
        self.__dictionary = dict()
        self.__handlesJson = HandlesJsonCache()

    def __checkForUpdate(self, last_update: int) -> bool:
        if int(time()) - last_update > 60:
            return True
        else:
            return False

    def __searchLastTime(self, key: str, value: str) -> bool:
        for v, last_time in self.__dictionary[key]:
            if value == v[1]:
                if self.__checkForUpdate(last_time):
                    return True
                else:
                    return False

        return False

    def __updateCache(self, key: str, value: str, version: int):
        if value == '' and version <= 0:
            del self.__dictionary[key]
        else:
            if key in self.__dictionary:
                del self.__dictionary[key]

            self.__dictionary[key] = [
                ((version, value), int(time()))
            ]  # key: [((version, value), time_insert_in_cache)]

    def __checkKeyCleanliness(self, key: str, value: str) -> None:
        for v, last_time in self.__dictionary[key]:
            if value == v[1]:
                self.__updateCache(key, '', -1)  # Cleans wrench
                break

    def insertAndUpdate(self, key: str, value: str) -> (str, str, int, int):
        key_returned, old_value_returned, old_version_returned, new_version_returned = (
            self.__handlesJson.Put(key, value)
        )

        if key in self.__dictionary:
            key_cache_returned, old_value, old_version = self.getByKeyVersion(key)

            if self.__searchLastTime(key, old_value):
                self.__checkKeyCleanliness(key, old_value)

                self.__dictionary[key].append(
                    ((new_version_returned, value), int(time()))
                )  # key: [((version, value), time_insert_in_cache)]

                return key_returned, old_value_returned, old_version_returned, new_version_returned
            else:
                return key_cache_returned, old_value, old_version, new_version_returned
        else:
            self.__dictionary[key] = [
                ((new_version_returned, value), int(time()))
            ]  # key: [((version, value), time_insert_in_cache)]

            return key_returned, old_value_returned, old_version_returned, new_version_returned

    def getByKeyVersion(self, key: str, version: int = -1) -> (str, str, int):
        valueSeach = ''
        versionSeach = -1
        lastUpdateData = -1

        if version <= 0:
            maxVersion = -1

            for k, v in self.__dictionary.items():
                if k == key:
                    for v0, v1 in v:
                        if v0[0] > maxVersion:
                            maxVersion = v0[0]
                            versionSeach = v0[0]
                            valueSeach = v0[1]
                            lastUpdateData = v1
                    break
        else:
            for k, v in self.returnDictionary().items():
                if k == key:
                    for v0, v1 in v:
                        if v0[0] <= version:
                            versionSeach = v0[0]
                            valueSeach = v0[1]
                            lastUpdateData = v1

        if not self.__checkForUpdate(lastUpdateData):  # The data is still valid
            if valueSeach == '' and versionSeach <= 0:
                return '', '', -1
            else:
                return key, valueSeach, versionSeach
        else:  # Data needs to be updated
            key_returned, value_returned, version_returned = self.__handlesJson.Get(key, version)
            self.__updateCache(key_returned, value_returned, version_returned)

            return key, valueSeach, versionSeach

    def getRangeByKeyVersion(self, start_key: str, end_key: str, start_version: float = -1,
                             end_version: float = -1) -> dict:
        values_in_range = dict()

        if start_version > 0 and end_version > 0:
            max_requested_version = max(start_version, end_version)

            for key, values in self.__dictionary.items():
                if start_key <= key <= end_key:
                    for version, value in values:
                        if version <= max_requested_version:
                            if key in values_in_range:
                                values_in_range[key].append((version, value))
                            else:
                                values_in_range[key] = [(version, value)]
        else:
            for k in list(self.__dictionary.keys()):
                if start_key <= k <= end_key:
                    _, value_returned, version_returned = self.getByKeyVersion(k)

                    if k in values_in_range:
                        values_in_range[k].append((version_returned, value_returned))
                    else:
                        values_in_range[k] = [(version_returned, value_returned)]

        return values_in_range

    def getAllInRange(self, key: str, version: int = -1) -> (int, str):
        if key in self.__dictionary:
            if version > 0:
                _, value_returned, version_returned = self.getByKeyVersion(key, version)
                return version_returned, value_returned
            else:
                _, value_returned, version_returned = self.getByKeyVersion(key)
                return version_returned, value_returned
        else:
            return -1, ''

    # Remove todos os valores associados à chave, exceto a versão mais recente, e retorna valor e versão para a chave
    def trim(self, key: str) -> (str, str, int):
        # key_returned, last_value, new_version = self.__handlesJson.Trim(key)

        if key in self.__dictionary:
            _, last_value, last_version = self.getByKeyVersion(key)

            self.__dictionary[key].clear()
            self.__dictionary[key].append((last_version, last_value))

            return key, last_value, last_version
        else:
            return '', '', -1

    def returnDictionary(self):
        return self.__dictionary

    def delete(self, key: str) -> (str, str, int):
        if key in self.__dictionary:
            key_cache_returned, value_cache_returned, verion_cache_returned = self.getByKeyVersion(key)

            del self.__dictionary[key]

        self.__handlesJson.Del(key)

        return key_cache_returned, value_cache_returned, verion_cache_returned

    def delRange(self, start_key: str, end_key: str) -> dict:
        values_in_range = dict()
        data_guard = True

        for k, list_values in self.__dictionary.items():
            if start_key <= k <= end_key:
                data_guard = True

                for version_value, value_value in list_values:
                    if data_guard:
                        if k in values_in_range:
                            values_in_range[k].append((version_value, value_value))
                        else:
                            values_in_range[k] = [(version_value, value_value)]

                        data_guard = False

        for key in list(values_in_range.keys()):
            del self.__dictionary[key]

        return values_in_range

    def delAll(self, key: str) -> list:
        if key in self.__dictionary:
            _, value_returned, version_returned = self.getByKeyVersion(key)

            del self.__dictionary[key]

            return key, value_returned, version_returned
        else:
            return '', '', -1


if __name__ == '__main__':
    d = ManipulateDictionary()

    print(d.insertAndUpdate('abudabi', 'fds'))
    print()
    print(d.insertAndUpdate('Rafael', 'Carlos'))
    print()
    print(d.insertAndUpdate('Carolina', 'Carla'))
    print()
    sleep(3)
    print(d.insertAndUpdate('Rafael', 'Carlos2'))
    print()
    print(d.getByKeyVersion('Rafael'))
    sleep(62)
    print(d.getByKeyVersion('Rafael'))

    # print(d.getByKeyVersion('Rafael'))
    # sleep(62)
    #
    # print(d.getByKeyVersion('Pietro'))
    # print(d.getByKeyVersion('Rafael'))

    print(d.returnDictionary())

    # for k, v in d.returnDictionary().items():
    #     print(k)
    #     for v1, v2 in v:
    #         print(f'\t {v1} e {v2}')
