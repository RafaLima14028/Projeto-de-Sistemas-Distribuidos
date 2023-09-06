from time import time, sleep


class ManipulateDictionary:
    __dictionary = None

    def __init__(self):
        self.__dictionary = dict()

    def insertAndUpdate(self, key: str, value: str) -> (str, str, int, int):
        # Retorno é dado por chave, valor, versão antiga se houver e a nova versão
        new_version = int(time())

        if key in self.__dictionary:
            # Busca a versão antiga
            old_value, old_version = self.getByKeyVersion(key)

            self.__dictionary[key].append((new_version, value))
            return key, old_value, old_version, new_version
        else:
            self.__dictionary[key] = [(new_version, value)]
            return key, '', -1, new_version

    def getByKeyVersion(self, key: str, version: float = -1) -> (str, float):
        valueSeach = ''
        versionSeach = -1

        if version <= 0:
            maxVersion = -1

            for k, v in self.__dictionary.items():
                if k == key:
                    for v0, v1 in v:
                        if v0 > maxVersion:
                            maxVersion = v0
                            versionSeach = v0
                            valueSeach = v1

            if valueSeach == '' and versionSeach == -1:
                return None, -1
            else:
                return valueSeach, versionSeach
        else:
            for k, v in self.returnDictionary().items():
                if k == key:
                    for v0, v1 in v:
                        if v0 <= version:
                            versionSeach = v0
                            valueSeach = v1

        if valueSeach == '' and versionSeach == -1:
            return None, -1
        else:
            return valueSeach, versionSeach

    def getRangeByKeyVersion(self, start_key: str, end_key: str, start_version: float = -1,
                             end_version: float = -1) -> dict:
        values_in_range = dict()

        if start_version > 0 and end_version > 0:
            if start_version > end_version:
                start_version, end_version = end_version, start_version

                max_requested_version = max(start_version, end_version)

                for key, values in self.__dictionary.items():
                    if start_key <= key <= end_key:
                        for version, value in values:
                            if version <= max_requested_version:
                                if key in values_in_range:
                                    values_in_range[key].append((version, value))
                                else:
                                    values_in_range[key] = [(version, value)]

            # for k, list_tuplas in self.__dictionary.items():  # Pega todos os elementos do dicionario
            #     if start_key <= k <= end_key:  # Verifica se a chave está no range
            #         for version_value, value_value in list_tuplas:  # Intera sobre as tuplas de uma chave
            #             if start_version <= version_value <= end_version:  # Verifica se a versão está no range desejado
            #                 if k in values_in_range:
            #                     values_in_range[k].append((version_value, value_value))
            #                 else:
            #                     values_in_range[k] = [(version_value, value_value)]
        else:
            for k in list(self.__dictionary.keys()):
                if start_key <= k <= end_key:
                    value_returned, version_returned = self.getByKeyVersion(k)

                    if k in values_in_range:
                        values_in_range[k].append((version_returned, value_returned))
                    else:
                        values_in_range[k] = [(version_returned, value_returned)]

        return values_in_range

    def getAllInRange(self, key: str, version: int = -1) -> (int, str):
        if key in self.__dictionary:
            if version > 0:
                value_returned, version_returned = self.getByKeyVersion(key, version)
                return version_returned, value_returned
            else:
                value_returned, version_returned = self.getByKeyVersion(key)
                return version_returned, value_returned
        else:
            return -1, ''

    # Remove todos os valores associados à chave, exceto a versão mais recente, e retorna valor e versão para a chave
    def trim(self, key: str) -> (str, str, int):
        if key in self.__dictionary:
            last_value, last_version = self.getByKeyVersion(key)

            self.__dictionary[key].clear()
            self.__dictionary[key].append((last_version, last_value))

            return key, last_value, last_version
        else:
            return key, '', -1

    def returnDictionary(self):
        return self.__dictionary

    def delete(self, key: str) -> (str, str, int):
        if key in self.__dictionary:
            last_value, last_version = self.getByKeyVersion(key)

            del self.__dictionary[key]

            return key, last_value, last_version
        else:
            return '', '', -1

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
        list_data_in_range = list()

        if key in self.__dictionary:
            value_returned, version_returned = self.getByKeyVersion(key)
            list_data_in_range.append((value_returned, version_returned))

            del self.__dictionary[key]

            return list_data_in_range
        else:
            return list_data_in_range


if __name__ == '__main__':
    dicionario = ManipulateDictionary()

    for i in range(3):
        print(dicionario.insertAndUpdate('A', f'Rafael'))
        sleep(.1)

    for i in range(3):
        print(dicionario.insertAndUpdate('B', f'Rafael2'))
        sleep(.1)

    for i in range(3):
        print(dicionario.insertAndUpdate('C', f'Rafael5'))
        sleep(.1)

    for i in range(5):
        print(dicionario.insertAndUpdate('D', f'Rafael5'))
        sleep(.1)

    for i in range(5):
        print(dicionario.insertAndUpdate('F', f'Rafael5'))
        sleep(.1)

    print(dicionario.getRangeByKeyVersion('A', 'F'))
    print(dicionario.returnDictionary())

    # start_time = float(input())
    # end_time = float(input())
    #
    # print()
    # print()
    # print()
    #
    # print(diconario.getRangeByKeyVersion('B', 'C', start_time, end_time))
