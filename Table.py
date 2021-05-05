import os
from bsddb3 import db
from errors import *


class Column:
    def __init__(self, dataType: str, name: str, maxLen=1000, isNotNull=False):
        self.dataType = dataType
        self.name = name
        self.isNotNull = isNotNull
        self.maxLen = maxLen
        self.beReferredCnt = 0

    def __str__(self):
        return "{}/{}/{}/{}/{}".format(self.dataType, self.name, self.maxLen, self.isNotNull, self.beReferredCnt)

    def copy(self, name=None):
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
        self.refTables = []
        self.originalTables = [] # for join
        self.originalColNames = [] # for join
        self.db = db.DB()
        self.filePath = './database/{}.db'.format(self.name)
        self.db.open(self.filePath, dbtype=db.DB_HASH, flags=db.DB_CREATE)
        if not self.db.get(b'cols'):
            self.db.put(b'cols', str(self.cols))
        if not self.db.get(b'pKeys'):
            self.db.put(b'pKeys', str(self.pKeys))
        if not self.db.get(b'fKeys'):
            self.db.put(b'fKeys', str(self.fKeys))

    def addCol(self, col: Column):
        # 기존 column 과 중복된 이름은 받지 않음.
        for existingCol in self.cols:
            if existingCol.name == col.name:
                self.drop()
                raise DuplicateColumnDefError('Create table has failed: column definition is duplicated')
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
                        self.drop()
                        raise DuplicatePrimaryKeyDefError('Create table has failed: primary key definition is duplicated')
                # primary key 는 자동적으로 not null
                col.isNotNull = True
                self.pKeys.append(col)
                dbLogStr = '['
                for pKey in self.pKeys:
                    dbLogStr += str(pKey) + ', '
                dbLogStr = dbLogStr[:-2]
                dbLogStr += ']'
                self.db.put(b'pKeys', dbLogStr)
                return
        self.drop()
        raise NonExistingColumnDefError(
                    "Create table has failed: '{}' does not exists in column definition".format(colName)
                )

    def setForeignKey(self, referredTable, fKeyInfoList: list):
        refPKeyNameList = []
        myFKeyNameList = []
        for fKeyInfo in fKeyInfoList:
            refPKeyNameList.append(fKeyInfo['referredColName'])
            myFKeyNameList.append(fKeyInfo['colName'])
        realRefPKeyNameList = []
        realRefColNameList = []
        for pKey in referredTable.pKeys:
            realRefPKeyNameList.append(pKey.name)
        for col in referredTable.cols:
            realRefColNameList.append(col.name)

        # ReferenceNonPrimaryKeyError, ReferenceColumnExistenceError check
        for referredColName in refPKeyNameList:
            if referredColName not in realRefColNameList:
                self.drop()
                raise ReferenceColumnExistenceError('Create table has failed: foreign key references non existing column')
            if referredColName not in realRefPKeyNameList:
                self.drop()
                raise ReferenceNonPrimaryKeyError('Create table has failed: foreign key references non primary key column')
        for pKeyName in realRefPKeyNameList:
            if pKeyName not in refPKeyNameList:
                self.drop()
                raise ReferenceNonPrimaryKeyError('Create table has failed: foreign key references non primary key column')

        for i in range(len(refPKeyNameList)):
            colName = myFKeyNameList[i]
            referredColName = refPKeyNameList[i]
            myCol = None
            for col in self.cols:
                if col.name == colName:
                    myCol = col
                    break
            # 해당 colName 이 없으면 Syntax Error 일으킴.
            if myCol is None:
                self.drop()
                raise NonExistingColumnDefError(
                    "Create table has failed: '{}' does not exists in column definition".format(colName)
                )
            refCol = None
            for pKey in referredTable.pKeys:
                if pKey.name == referredColName:
                    refCol = pKey
                    break
            # 타입 일치하는지 확인.
            if not myCol.dataType == refCol.dataType:
                self.drop()
                raise ReferenceTypeError('Create table has failed: foreign key references wrong type')
            # char type 일 때 길이까지 확인.
            if myCol.dataType == 'char' and myCol.maxLen != refCol.maxLen:
                self.drop()
                raise ReferenceTypeError('Create table has failed: foreign key references wrong type')

            newFKeyInfo = dict()
            newFKeyInfo['column'] = colName
            newFKeyInfo['referredTableName'] = referredTable.name
            newFKeyInfo['referredColName'] = refCol.name
            refCol.beReferredCnt += 1
            self.refTables.append(referredTable)
            self.fKeys.append(newFKeyInfo)
            self.db.put(b'fKeys', str(self.fKeys))

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
            raise SyntaxError
        newTable = Table(alias)
        # isReferred = False 로 만들기 위해.
        for col in self.cols:
            newTable.cols.append(col.copy())
        # pKeys 에 들어가는 건 cols 에 있는 것과 똑같은 객체이므로.
        for pKey in self.pKeys:
            for col in newTable.cols:
                if col.name == pKey.name:
                    newTable.pKeys.append(col)
                    break
        newTable.fKeys = self.fKeys
        newTable.rows = self.rows
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

    def showInfo(self):
        maxLen = 0
        foreignKeyNames = []
        for fKeyInfo in self.fKeys:
            foreignKeyNames.append(fKeyInfo['column'])

        colInfos = []
        for col in self.cols:
            nameLen = len(col.name)
            if maxLen < nameLen:
                maxLen = nameLen
            dataType = col.dataType
            if dataType == 'char':
                dataType = f'char({col.maxLen})'
            nullable = 'Y'
            if col.isNotNull:
                nullable = 'N'
            keyType = ''
            if col in self.pKeys:
                keyType = 'PRI'
                if col.name in foreignKeyNames:
                    keyType += '/FOR'
            else:
                if col.name in foreignKeyNames:
                    keyType = 'FOR'
            colInfos.append('{:16}{:12}{:12}'.format(dataType, nullable, keyType))
            pass
        print('-------------------------------------------------')
        print('table_name [{}]'.format(self.name))
        tabSize = max((2 + (maxLen - 1) // 4) * 4, 16) - len('column_name')
        tabbed = 'column_name' + ' ' * tabSize
        print('{}{:16}{:12}{:12}'.format(tabbed, 'type', 'null', 'key'))
        for i in range(len(self.cols)):
            col = self.cols[i]
            colInfo = colInfos[i]
            tabSize = max((2 + (maxLen-1)//4)*4, 16) - len(col.name)
            tabbedName = col.name + ' ' * tabSize
            print('{}{}'.format(tabbedName, colInfo))
        print('-------------------------------------------------')

    # db file close
    def closeFile(self):
        self.db.close()

    # remove db file
    def drop(self):
        for col in self.cols:
            if col.beReferredCnt != 0:
                raise DropReferencedTableError(f"Drop table has failed: '{self.name}' is referenced by other table")
        # referred table 에서 isReferred = False 로 변경.
        for fKeyInfo in self.fKeys:
            refTableName = fKeyInfo['referredTableName']
            refColName = fKeyInfo['referredColName']
            for table in self.refTables:
                if table.name == refTableName:
                    for col in table.cols:
                        if col.name == refColName:
                            col.beReferredCnt -= 1
                            break
                    break
        self.db.close()
        os.remove('./database/{}.db'.format(self.name))
