from time import time, sleep


class ManipulateDictionary:
    __dictionary = None

    def __init__(self):
        self.__dictionary = dict()

    def insertAndUpdate(self, key: str, value: str) -> (str, str, int, int):
        # Retorno é dado por chave, valor, versão antiga se houver e a nova versão
        new_version = time()

        if key in self.__dictionary:
            # Busca a versão antiga
            # old_version = self.__dictionary[key][0][0]
            old_version = self.getByKeyVersion(key)[1]

            self.__dictionary[key].append((new_version, value))
            return key, value, old_version, new_version
        else:
            self.__dictionary[key] = [(new_version, value)]
            return key, value, -1, new_version

    def getByKeyVersion(self, key: str, version: float = -1):
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
                return None, None
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
            return None, None
        else:
            return valueSeach, versionSeach

    def returnDictionary(self):
        return self.__dictionary


if __name__ == '__main__':
    diconario = ManipulateDictionary()

    for i in range(10):
        print(diconario.insertAndUpdate('A', f'Rafael'))
        sleep(.5)

    # new_time = float(input(''))
    print(diconario.getByKeyVersion('A'))

    # for k, v in diconario.returnDictionary().items():
    #     # for v0, v1 in v:
    #     print(k)

    # print(diconario.getByKeyVersion('A', 169279))
