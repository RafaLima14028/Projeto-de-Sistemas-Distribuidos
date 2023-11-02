from time import time, sleep
import socket

from src.handlesJSON import HandlesJsonCache


class ManipulateDictionary:
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

    def __updateCache(self, key: str, value: str, version: int) -> None:
        if value == '' and version <= 0:
            if key in self.__dictionary:
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
        if key in self.__dictionary:
            key_cache_returned, old_value, old_version = self.getByKeyVersion(key)

            key_returned, old_value_returned, old_version_returned, new_version_returned = (
                self.__handlesJson.Put(key, value)
            )

            if self.__searchLastTime(key, old_value):
                self.__checkKeyCleanliness(key, old_value)

                self.__dictionary[key].append(
                    ((new_version_returned, value), int(time()))
                )  # key: [((version, value), time_insert_in_cache)]

                return key_returned, old_value_returned, old_version_returned, new_version_returned
            else:
                return key_cache_returned, old_value, old_version, new_version_returned
        else:
            key_returned, old_value_returned, old_version_returned, new_version_returned = (
                self.__handlesJson.Put(key, value)
            )

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

            return key_returned, value_returned, version_returned

    def getRangeByKeyVersion(self, start_key: str, end_key: str, start_version: float = -1,
                             end_version: float = -1) -> dict:
        values_in_range = dict()
        need_refresh = False

        if start_version > 0 and end_version > 0:
            max_requested_version = max(start_version, end_version)

            for key, values in self.__dictionary.items():
                if start_key <= key <= end_key:
                    for values, last_time_update in values:
                        if self.__checkForUpdate(last_time_update):  # Need to refresh the cache with the data
                            need_refresh = True
                            break

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

        if need_refresh:
            new_dict = self.__handlesJson.GetRange(start_key, end_key, start_version, end_version)

            for k, v in new_dict:
                self.__updateCache(k, v[1], v[0])

            return new_dict
        else:
            return values_in_range

    def trim(self, key: str) -> (str, str, int):
        last_key, last_value, last_version = self.getByKeyVersion(key)

        if key in self.__dictionary:
            self.__dictionary[key].clear()
            self.__dictionary[key].append((last_version, last_value))

        self.__handlesJson.Trim(key)

        return last_key, last_value, last_version

    def returnDictionary(self):
        return self.__dictionary

    def delete(self, key: str) -> (str, str, int):
        key_cache_returned, value_cache_returned, verion_cache_returned = (
            self.getByKeyVersion(key)
        )

        if key in self.__dictionary:
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
