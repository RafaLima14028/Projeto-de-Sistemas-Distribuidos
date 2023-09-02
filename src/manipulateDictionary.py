from time import time, sleep


class ManipulateDictionary:
    __dictionary = None

    def __init__(self):
        self.__dictionary = dict()

    def insertAndUpdate(self, key: str, value: str) -> (str, str, float, float):
        # Retorno é dado por chave, valor, versão antiga se houver e a nova versão
        new_version = time()

        if key in self.__dictionary:
            # Busca a versão antiga
            old_version = self.getByKeyVersion(key)[1]

            self.__dictionary[key].append((new_version, value))
            return key, value, old_version, new_version
        else:
            self.__dictionary[key] = [(new_version, value)]
            return key, value, -1, new_version

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

        for k, list_tuplas in self.__dictionary.items():
            if start_key <= k <= end_key:
                values_in_range[k] = {tupla for tupla in list_tuplas if
                                      start_version <= tupla[0] <= end_version}

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

    print(diconario.returnDictionary())

    start_time = float(input())
    end_time = float(input())

    print(diconario.getRangeByKeyVersion('A', 'B', start_time, end_time))
