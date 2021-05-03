import os
from bsddb3 import db


class Column:
    def __init__(self, dataType: str, name: str, maxLen=1000, isNotNull=False):
        self.dataType = dataType
        self.name = name
        self.isNotNull = isNotNull
        self.maxLen = maxLen

    def __str__(self):
        return "{}/{}/{}/{}".format(self.dataType, self.name, self.maxLen, self.isNotNull)

    def copy(self, name: str):
        if name is None:
            name = self.name
        return Column(self.dataType, name, self.maxLen, self.isNotNull)


class Table:
    def __init__(self, name: str):
        self.name = name
        self.cols = []
        self.rows = []
        self.pKeys = []
        self.fKeys = []
        self.originalTables = []
        self.originalColNames = []
        self.db = db.DB()
        self.db.open('./database/{}.db'.format(self.name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
        if not self.db.get(b'cols'):
            self.db.put(b'cols', str(self.cols))
        if not self.db.get(b'pKeys'):
            self.db.put(b'pKeys', str(self.pKeys))
        if not self.db.get(b'fKeys'):
            self.db.put(b'fKeys', str(self.fKeys))

    def addCol(self, col: Column):
        # 기존 column 과 중복된 이름은 받지 않음.
        for existingCol in self.cols:
            assert existingCol.name != col.name
        self.cols.append(col)
        dbLogStr = '['
        for col in self.cols:
            dbLogStr += str(col) + ', '
        dbLogStr = dbLogStr[:-2]
        dbLogStr += ']'
        self.db.put(b'cols', dbLogStr)

    def setPrimaryKey(self, colName: str):
        # 해당 colName 이 없으면 Syntax Error 일으킴.
        for col in self.cols:
            if col.name == colName:
                # 이미 들어가 있을 경우를 위해 한 번 체크
                for pKey in self.pKeys:
                    if pKey.name == colName:
                        return
                self.pKeys.append(col)
                dbLogStr = '['
                for pKey in self.pKeys:
                    dbLogStr += str(pKey) + ', '
                dbLogStr = dbLogStr[:-2]
                dbLogStr += ']'
                self.db.put(b'pKeys', dbLogStr)
                return
        raise SyntaxError

    def setForeignKey(self, colName: str, referredTableName, referredColName):
        # 해당 colName 이 없으면 Syntax Error 일으킴.
        for col in self.cols:
            if col.name == colName:
                newFKeyInfo = dict()
                newFKeyInfo['column'] = colName
                # 이미 들어가 있을 경우를 위해 한 번 체크
                for fKey in self.fKeys:
                    if fKey['column'].name == colName:
                        return
                newFKeyInfo['referredTableName'] = referredTableName
                newFKeyInfo['referredColName'] = referredColName
                self.fKeys.append(newFKeyInfo)
                self.db.put(b'fKeys', str(self.fKeys))
                return
        raise SyntaxError

    def addRow(self, rowInfo: dict):
        row = dict()
        for col in self.cols:
            data = rowInfo.setdefault(col.name, dict())
            # 해당 column 정보가 없을 경우
            if len(data.keys()) == 0 and col.isNotNull:
                raise ValueError
            # data type 이 안 맞을 경우
            if data.setdefault('type', None) != col.dataType:
                raise ValueError
            # 해당 data 가 null 일 경우
            value = data.setdefault('value', None)
            if value is None and col.isNotNull:
                raise ValueError
            row[col.name] = value
        # private key인데 똑같은 key 조합이 있는지 확인
        isAlreadyExistPKey = False
        for existingRow in self.rows:
            isSamePKey = True
            for pKey in self.pKeys:
                isSamePKey &= existingRow[pKey.name] == row[pKey.name]
            if isSamePKey:
                isAlreadyExistPKey = True
                break
        if isAlreadyExistPKey:
            raise SyntaxError
        keyStr = ''
        for pKey in self.pKeys:
            tmpKey = str(row[pKey.name])
            key = ''
            for c in tmpKey:
                if c == '/':
                    key += '//'
                else:
                    key += c
            keyStr += key + '/'
        self.rows.append(row)
        self.db.put(keyStr.encode(), str(row))

    def addOriginalTable(self, table):
        if len(table.originalTables) == 0:
            self.originalTables.append(table)
        else:
            for originalTable in table.originalTables:
                self.originalTables.append(originalTable)

    def copy(self, alias):
        if alias is None:
            alias = self.name
        newTable = Table(alias)
        newTable.cols = self.cols
        newTable.rows = self.rows
        newTable.pKeys = self.pKeys
        newTable.fKeys = self.fKeys
        return newTable

    def changeColName(self, originalName, newName):
        for i in range(len(self.cols)):
            col = self.cols[i]
            if col.name == originalName:
                # 먼저 다른 컬럼이랑 이름이 중복되지 않아야 함.
                for j in range(len(self.cols)):
                    if i == j:
                        continue
                    elif newName == self.cols[j].name:
                        return False
                col.name = newName
                self.originalColNames.append({'originalName': originalName, 'newName': newName})
                for row in self.rows:
                    row[newName] = row[originalName]
                    del row[originalName]
                return True
        return False

    def _getNewName(self, originalName):
        for history in self.originalColNames:
            if history['originalName'] == originalName:
                return history['newName']
        return None

    # as 때문에 col 이름이 바뀌었을 때 해당 항목을 찾아주는.
    def findColName(self, colName):
        output = colName
        isIn = False
        while not isIn:
            assert output is not None
            for col in self.cols:
                if col.name == output:
                    return col.name
            if not isIn:
                nextValue = self._getNewName(output)
                if nextValue is None:
                    splited = output.split('.')
                    for col in self.cols:
                        if col.name == splited[-1]:
                            return col.name

                    if len(splited) != 1:
                        nextValue = self._getNewName(splited[-1])
                output = nextValue
        return output

    # db file close
    def closeFile(self):
        self.db.close()

    def drop(self):
        self.closeFile()
        os.remove('./database/{}.db'.format(self.name))
