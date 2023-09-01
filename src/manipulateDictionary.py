from time import time, sleep


class ManipulateDictionary:
    __dictionary = None

    def __init__(self):
        self.__dictionary = dict()

    def insert(self, key: str, value: str):
        if key in self.__dictionary:
            self.__dictionary[key].append((time(), value))
        else:
            self.__dictionary[key] = [(time(), value)]

    def getByKeyVersion(self, key, version=-1.0):
        valueSeach = ''
        versionSeach = -1

        if version <= 0:
            maxVersion = -1

            for k, v in self.returnDictionary().items():
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
    diconario.insert('A', 'Rafael')
    sleep(2)
    diconario.insert('A', 'Rafael2')
    sleep(2)
    diconario.insert('A', 'Rafael2')
    sleep(2)
    diconario.insert('A', 'Rafael2')
    sleep(2)
    diconario.insert('A', 'Rafael2')
    sleep(2)
    diconario.insert('A', 'Rafael2')
    sleep(2)
    diconario.insert('A', 'Rafael10')

    for k, v in diconario.returnDictionary().items():
        # for v0, v1 in v:
        print(k)

    # print(diconario.getByKeyVersion('A', 169279))
