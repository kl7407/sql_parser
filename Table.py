import os
from bsddb3 import db
from errors import *


class Column:
    def __init__(self, dataType: str, name: str, label=None, maxLen=1000, isNotNull=False):
        self.dataType = dataType
        self.name = name  # 모든 column 의 이름이 다름.
        self.label = label
        self.maxLen = maxLen
        self.isNotNull = isNotNull
        self.beReferredCnt = 0

    def __str__(self):
        label = self.label
        if label is None:
            label = ""
        return f"{self.dataType}/{self.name}/{label}/{self.maxLen}/{self.isNotNull}/{self.beReferredCnt}"

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
        self.refMeTables = []
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
        '''
        for pKeyName in realRefPKeyNameList:
            if pKeyName not in refPKeyNameList:
                self.drop()
                raise ReferenceNonPrimaryKeyError(
                    'Create table has failed: foreign key references non primary key column'
                )
        '''

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
            referredTable.refMeTables.append(self)
            self.fKeys.append(newFKeyInfo)
            if self.db is not None:
                self.db.put(b'fKeys', str(self.fKeys))

    def getRefTableByName(self, tableName):
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
            refTable = self.getRefTableByName(refTableName)
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
            if len(data.keys()) == 0:
                raise InsertTypeMismatchError("Insertion has failed: Types are not matched")
            # data type 이 안 맞을 경우
            if data.setdefault('type', None) != col.dataType:
                raise InsertTypeMismatchError('Insertion has failed: Types are not matched')
            # 해당 data 가 null 일 경우 nullable check
            value = data.setdefault('value', None)
            if value is None and col.isNotNull:
                label = col.label
                if label is None:
                    label = col.name.split('.')[-1]
                raise InsertColumnNonNullableError(f"Insertion has failed: '{label}' is not nullable")
            # char_일 경우 maxLen_보다 더 긴 값이 들어올 경우 잘라줌.
            if col.dataType == 'char' and (value is not None) and len(value) > col.maxLen:
                value = value[:col.maxLen]
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
        """
        :return Bool
        """
        # 먼저 해당 row 를 참조하고 있는 row_가 있는지 확인함.
        if row.setdefault("_isReferred", 0) != 0:
            # 참조하는 row_가 있으면 해당 row_들을 찾고 nullable 여부 확인함.
            beUpdatedRowInfos = []
            for table in self.refMeTables:
                myColNames = []
                itsColNames = []
                for itsFKey in table.fKeys:
                    if itsFKey['referredTableName'] == self.name:
                        if itsFKey['referredColName'] not in myColNames:
                            myColNames.append(itsFKey['referredColName'])
                            itsColNames.append(itsFKey['column'])
                for itsRow in table.rows:
                    isReferMe = len(myColNames) != 0
                    for i in range(len(myColNames)):
                        myValue = row[myColNames[i]]
                        itsValue = itsRow[itsColNames[i]]
                        isReferMe &= myValue == itsValue
                    if isReferMe:
                        # 만약 row_를 참조하고 있다면 -> 참조하는 itsCol_이 non-nullable 이라면 안 지음.
                        for itsColName in itsColNames:
                            for itsCol in table.cols:
                                if itsCol.name == itsColName:
                                    if itsCol.isNotNull:
                                        return False
                        # 다 nullable 이라면 beUpdatedRowInfo_에 넣어줌.
                        beUpdatedRowInfos.append((itsRow, itsColNames))

            for beUpdatedRowInfo in beUpdatedRowInfos:
                itsRow = beUpdatedRowInfo[0]
                itsColNames = beUpdatedRowInfo[1]
                for itsColName in itsColNames:
                    itsRow[itsColName] = None


        self.db.delete(self._getKeyStr(row).encode())
        self.rows.remove(row)
        for fKeyInfo in self.fKeys:
            myColName = fKeyInfo['column']
            refTableName = fKeyInfo['referredTableName']
            refColName = fKeyInfo['referredColName']
            refTable = self.getRefTableByName(refTableName)
            if refTable is None:
                # 이런 경우는 없으므로
                raise SyntaxError
            if row[myColName] is None:
                # value == null 이면 참조한 게 없으므로 _isReferred 바꿀 필요가 없음.
                continue
            # value 가 있을 경우
            for refRow in refTable.rows:
                if refRow[refColName] == row[myColName]:
                    refRow['_isReferred'] -= 1
                    refTable.updateRowAtDB(refRow)
                    break
        return True

    def updateRowAtDB(self, row):
        keyStr = self._getKeyStr(row)
        if self.db is not None:
            self.db.put(keyStr.encode(), str(row))

    def copy(self, alias):
        # join 에서 가명으로 불릴 때를 위해 사용함. 임시로 사용되는 것이므로 db file 만들 필요 없음(self.initDBFile() X).
        # pKey, fKey 다 필요없
        if (alias is None) or (type(alias) is not str):
            raise SyntaxError
        newTable = Table(alias, False)
        # isReferred = False 로 만들기 위해.
        for col in self.cols:
            colNameWithoutTableName = col.name[len(self.name)+1:]
            newTable.cols.append(col.copy(name=f'{alias}.{colNameWithoutTableName}'))
        '''
        # pKeys 에 들어가는 건 cols 에 있는 것과 똑같은 객체이므로.
        for pKey in self.pKeys:
            for col in newTable.cols:
                colNameWithoutTableName = col.name[len(self.name) + 1:]
                if col.name == pKey.name:
                    newTable.setPrimaryKey(f'{alias}.{colNameWithoutTableName}')
                    break
        newTable.didSetPKeys = True

        for fKey in self.fKeys:
            newFKey = dict()
            newFKey['column'] = f"{alias}.{fKey['column']}"
            newFKey['referredTableName'] = fKey['referredTableName']
            newFKey['referredColName'] = fKey['referredColName']
            newTable.fKeys.append(newFKey)
        
        for refTable in self.refTables:
            newTable.refTables.append(refTable)
        '''
        newTable.rows = []
        for row in self.rows:
            newRow = dict()
            for col in self.cols:
                colNameWithoutTableName = col.name[len(self.name) + 1:]
                newRow[f'{alias}.{colNameWithoutTableName}'] = row[col.name]
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
            label = col.label
            if label is None:
                label = col.name.split('.')[1]
            labelLen = len(label)
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
            label = col.label
            if label is None:
                label = col.name.split('.')[1]
            tabSize = max((2 + (maxLen-1)//4)*4, 16) - len(label)
            tabbedName = label + ' ' * tabSize
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
        # referred table_에서 isReferred = False 로 변경.
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
        if self.db is not None:
            self.db.close()
            os.remove(f'./database/{self.name}.db')
