class Column:
    def __init__(self, dataType: str, name: str, maxLen=1000, isNotNull=False):
        self.dataType = dataType
        self.name = name
        self.isNotNull = isNotNull
        self.maxLen = maxLen


class Table:
    def __init__(self, name: str):
        self.name = name
        self.cols = []
        self.rows = []
        self.pKeys = []
        self.fKeys = []

    def addCol(self, col: Column):
        # 기존 column 과 중복된 이름은 받지 않음.
        for existingCol in self.cols:
            assert existingCol.name != col.name
        self.cols.append(col)

    def setPrimaryKey(self, colName: str):
        # 해당 colName 이 없으면 Syntax Error 일으킴.
        for col in self.cols:
            if col.name == colName:
                # 이미 들어가 있을 경우를 위해 한 번 체크
                for pKey in self.pKeys:
                    if pKey.name == colName:
                        return
                self.pKeys.append(col)
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
        self.rows.append(row)

