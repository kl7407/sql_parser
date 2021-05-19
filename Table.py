import os
from bsddb3 import db
from errors import *


class Column:
    def __init__(self, dataType: str, name: str, label: str, maxLen=1000, isNotNull=False):
        self.dataType = dataType
        self.name = name  # 모든 column 의 이름이 다름.
        self.label = label
        self.maxLen = maxLen
        self.isNotNull = isNotNull
        self.beReferredCnt = 0
        # TODO: 현재 컬럼 이름을 바꿔서 select output 에 사용하는데, 이름을 바꾸는 게 아니라 label 을 추가해서 출력할 때 바꾸도록 해야함.
        # TODO: 또한 현재 상태에서는 join 안 한 table 에 select as 사용하면 column name 이 싹 바뀌는데 이것도 해결해야 함.

    def __str__(self):
        return f"{self.dataType}/{self.name}/{self.label}/{self.maxLen}/{self.isNotNull}/{self.beReferredCnt}"

    def copy(self, name=None, label=None):
        if name is None:
            name = self.name
        if label is None:
            label = self.label
        return Column(dataType=self.dataType, name=name, maxLen=self.maxLen, isNotNull=self.isNotNull,
                      label=label)


class Table:
    def __init__(self, name: str, initDB=True):
        self.name = name
        self.cols = []
        self.rows = []
        self.pKeys = []
        self.didSetPKeys = False
        self.fKeys = []
        self.refTables = []
        self.originalTables = []  # for join
        self.db = None
        self.filePath = f'./database/{self.name}.db'
        if initDB:
            self.initDBFile()

    def initDBFile(self):
        self.db = db.DB()
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
        if self.db is not None:
            self.db.put(b'cols', dbLogStr)

    def setPrimaryKey(self, colName: str):
        # 이전에 primary key 설정한 적이 있으면 에러 일으킴.
        if self.didSetPKeys:
            self.drop()
            raise DuplicatePrimaryKeyDefError('Create table has failed: primary key definition is duplicated')
        # 해당 colName 이 없으면 Syntax Error 일으킴.
        for col in self.cols:
            if col.name == colName:
                # 이미 들어가 있을 경우를 위해 한 번 체크
                for pKey in self.pKeys:
                    if pKey.name == colName:
                        self.drop()
                        raise DuplicatePrimaryKeyDefError(
                            'Create table has failed: primary key definition is duplicated'
                        )
                # primary key 는 자동적으로 not null
                col.isNotNull = True
                self.pKeys.append(col)
                dbLogStr = '['
                for pKey in self.pKeys:
                    dbLogStr += str(pKey) + ', '
                dbLogStr = dbLogStr[:-2]
                dbLogStr += ']'
                if self.db is not None:
                    self.db.put(b'pKeys', dbLogStr)
                return
        self.drop()
        raise NonExistingColumnDefError(f"Create table has failed: '{colName}' does not exists in column definition")

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
                raise ReferenceColumnExistenceError(
                    'Create table has failed: foreign key references non existing column'
                )
            if referredColName not in realRefPKeyNameList:
                self.drop()
                raise ReferenceNonPrimaryKeyError(
                    'Create table has failed: foreign key references non primary key column'
                )
        for pKeyName in realRefPKeyNameList:
            if pKeyName not in refPKeyNameList:
                self.drop()
                raise ReferenceNonPrimaryKeyError(
                    'Create table has failed: foreign key references non primary key column'
                )

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
                    f"Create table has failed: '{colName}' does not exists in column definition"
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
            if self.db is not None:
                self.db.put(b'fKeys', str(self.fKeys))

    def _getRefTableByName(self, tableName):
        for table in self.refTables:
            if table.name == tableName:
                return table
        return None

    def _checkRefIntegrityConstraint(self, rowInfo: dict):
        isAlright = True
        for fKeyInfo in self.fKeys:
            myColName = fKeyInfo['column']
            refTableName = fKeyInfo['referredTableName']
            refColName = fKeyInfo['referredColName']
            refTable = self._getRefTableByName(refTableName)
            if refTable is None:
                raise SyntaxError
            isIn = False
            fieldValue = rowInfo[f'{myColName}']['value']
            # Nullable 여부는 앞에서 체크하므로 여기서는 rowInfo value == Null 이면 값 비교 안하고 continue
            if fieldValue is None:
                continue
            for refRow in refTable.rows:
                refFieldValue = refRow[refColName]
                if refFieldValue == fieldValue:
                    refRow['_isReferred'] = refRow.setdefault('_isReferred', 0) + 1
                    isIn = True
                    break
            if not isIn:
                isAlright = False
                break
        return isAlright

    def _getKeyStr(self, row: dict):
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
        return keyStr

    def addRow(self, rowInfo: dict):
        row = dict()
        for col in self.cols:
            data = rowInfo.setdefault(col.name, dict())
            # 해당 column 정보가 없을 경우
            if len(data.keys()) == 0 and col.isNotNull:
                raise InsertColumnNonNullableError(
                    f"Insertion has failed: '[{col.label}]' is not nullable")
            # data type 이 안 맞을 경우
            if data.setdefault('type', None) != col.dataType:
                raise InsertTypeMismatchError('Insertion has failed: Types are not matched')
            # 해당 data 가 null 일 경우
            value = data.setdefault('value', None)
            if value is None and col.isNotNull:
                raise InsertColumnNonNullableError(f"Insertion has failed: '{col.label}' is not nullable")
            row[col.name] = value
        # reference integrity constraint 확인
        if not self._checkRefIntegrityConstraint(rowInfo):
            raise InsertReferentialIntegrityError("Insertion has failed: Referential integrity violation")
        # private key 인데 똑같은 key 조합이 있는지 확인
        isAlreadyExistPKey = False
        for existingRow in self.rows:
            isSamePKey = True
            for pKey in self.pKeys:
                fieldKey = pKey.name
                isSamePKey &= existingRow[fieldKey] == row[fieldKey]
            if isSamePKey:
                isAlreadyExistPKey = True
                break
        if isAlreadyExistPKey:
            raise InsertDuplicatePrimaryKeyError("Insertion has failed: Primary key duplication")
        keyStr = self._getKeyStr(row)
        self.rows.append(row)
        if self.db is not None:
            self.db.put(keyStr.encode(), str(row))

    def deleteRow(self, row):
        self.db.delete(self._getKeyStr(row).encode())
        self.rows.remove(row)
        for fKeyInfo in self.fKeys:
            myColName = fKeyInfo['column']
            refTableName = fKeyInfo['referredTableName']
            refColName = fKeyInfo['referredColName']
            refTable = self._getRefTableByName(refTableName)
            if refTable is None:
                raise SyntaxError
            if row[myColName] is None:
                continue
            for refRow in refTable.rows:
                if refRow[refColName] == row[myColName]:
                    refRow['_isReferred'] -= 1
                    refTable.updateRowAtDB(refRow)
                    break

    def updateRowAtDB(self, row):
        keyStr = self._getKeyStr(row)
        if self.db is not None:
            self.db.put(keyStr.encode(), str(row))

    def addOriginalTable(self, table):
        if len(table.originalTables) == 0:
            self.originalTables.append(table)
        else:
            for originalTable in table.originalTables:
                self.originalTables.append(originalTable)

    def findColWithOriginalTableName(self, colName):
        outputs = []
        for originalTable in self.originalTables:
            for col in originalTable.cols:
                if colName == f'{originalTable.name}.{col.name}':
                    outputs.append(col)
        cnt = len(outputs)
        if cnt == 0:
            return None
        elif cnt == 1:
            return outputs[0]
        else:
            raise SyntaxError

    def copy(self, alias):
        # join 에서 가명으로 불릴 때를 위해 사용함. 임시로 사용되는 것이므로 db file 만들 필요 없음(self.initDBFile() X).
        if (alias is None) or (type(alias) is not str):
            raise SyntaxError
        newTable = Table(alias, False)
        # isReferred = False 로 만들기 위해.
        for col in self.cols:
            newTable.cols.append(col.copy(name=f'{alias}.{col.name}'))
        # pKeys 에 들어가는 건 cols 에 있는 것과 똑같은 객체이므로.
        for pKey in self.pKeys:
            for col in newTable.cols:
                if col.name == pKey.name:
                    newTable.setPrimaryKey(f'{alias}.{col.name}')
                    break
        newTable.didSetPKeys = True

        for fKey in self.fKeys:
            newFKey = dict()
            newFKey['column'] = f"{alias}.{fKey['column']}"
            newFKey['referredTableName'] = fKey['referredTableName']
            newFKey['referredColName'] = fKey['referredColName']
            newTable.fKeys.append(newFKey)
        newTable.rows = []
        for refTable in self.refTables:
            newTable.refTables.append(refTable)
        for ori in self.originalTables:
            newTable.originalTables.append(ori)

        for row in self.rows:
            newRow = dict()
            for col in self.cols:
                newRow[f'{alias}.{col.name}'] = row[f'{self.name}.{col.name}']
            newTable.rows.append(newRow)
        return newTable

    # select 에서 출력될 때 어떤 명칭으로 불릴 것인지를 정함. as 때문에 필요
    def setColLabel(self, originalLabel, newLabel):
        for i in range(len(self.cols)):
            col = self.cols[i]
            if col.label == originalLabel:
                col.label = newLabel
                return True
        return False

    def showInfo(self):
        maxLen = 0
        foreignKeyNames = []
        for fKeyInfo in self.fKeys:
            foreignKeyNames.append(fKeyInfo['column'])

        colInfos = []
        for col in self.cols:
            labelLen = len(col.label)
            if maxLen < labelLen:
                maxLen = labelLen
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
        print(f'table_name [{self.name}]')
        tabSize = max((2 + (maxLen - 1) // 4) * 4, 16) - len('column_name')
        tabbed = 'column_name' + ' ' * tabSize
        print('{}{:16}{:12}{:12}'.format(tabbed, 'type', 'null', 'key'))
        for i in range(len(self.cols)):
            col = self.cols[i]
            colInfo = colInfos[i]
            tabSize = max((2 + (maxLen-1)//4)*4, 16) - len(col.label)
            tabbedName = col.label + ' ' * tabSize
            print(f'{tabbedName}{colInfo}')
        print('-------------------------------------------------')

    # db file close
    def closeFile(self):
        if self.db is None:
            return
        dbLogStr = '['
        for col in self.cols:
            dbLogStr += str(col) + ', '
        dbLogStr = dbLogStr[:-2]
        dbLogStr += ']'
        self.db.put(b'cols', dbLogStr)

        dbLogStr = '['
        for pKey in self.pKeys:
            dbLogStr += str(pKey) + ', '
        dbLogStr = dbLogStr[:-2]
        dbLogStr += ']'
        self.db.put(b'pKeys', dbLogStr)

        self.db.put(b'fKeys', str(self.fKeys))

        for row in self.rows:
            self.updateRowAtDB(row)

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
        self.closeFile()
        os.remove(f'./database/{self.name}.db')
