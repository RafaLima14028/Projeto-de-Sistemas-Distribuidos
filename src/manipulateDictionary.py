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
        if start_version > 0 and end_version > 0 and end_version > start_version:
            tmp = start_version
            start_version = end_version
            end_version = tmp
        if end_key < start_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        values_in_range = dict()

        if start_version > 0 and end_version > 0:
            for k, list_tuplas in self.__dictionary.items():  # Pega todos os elementos do dicionario
                if start_key <= k <= end_key:  # Verifica se a chave está no range
                    for version_value, value_value in list_tuplas:  # Intera sobre as tuplas de uma chave
                        if start_version <= version_value <= end_version:  # Verifica se a versão está no range desejado
                            if k in values_in_range:
                                values_in_range[k].append((version_value, value_value))
                            else:
                                values_in_range[k] = [(version_value, value_value)]
        else:
            for k, list_tuplas in self.__dictionary.items():  # Pega todos os elementos do dicionario
                if start_key <= k <= end_key:  # Verifica se a chave está no range
                    for version_value, value_value in list_tuplas:  # Intera sobre as tuplas de uma chave
                        if k in values_in_range:
                            values_in_range[k].append((version_value, value_value))
                        else:
                            values_in_range[k] = [(version_value, value_value)]

        return values_in_range

    # Remove todos os valores associados à chave, exceto a versão mais recente, e retorna valor e versão para a chave
    def trim(self, key: str) -> (str, str, float):
        if key in self.__dictionary:
            last_value, last_version = self.getByKeyVersion(key)

            self.__dictionary[key].clear()
            self.__dictionary[key].append((last_version, last_value))

            return key, last_value, last_version

    def returnDictionary(self):
        return self.__dictionary


if __name__ == '__main__':
    diconario = ManipulateDictionary()

    for i in range(3):
        print(diconario.insertAndUpdate('A', f'Rafael'))
        sleep(.5)

    for i in range(3):
        print(diconario.insertAndUpdate('B', f'Rafael2'))
        sleep(.5)

    for i in range(3):
        print(diconario.insertAndUpdate('C', f'Rafael5'))
        sleep(.5)

    print(diconario.getRangeByKeyVersion('A', 'C'))

    # start_time = float(input())
    # end_time = float(input())
    #
    # print()
    # print()
    # print()
    #
    # print(diconario.getRangeByKeyVersion('B', 'C', start_time, end_time))
