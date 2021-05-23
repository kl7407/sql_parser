from lark import *
from Table import *
from errors import *

with open('grammar.lark') as file:
    fileInfo = file.read()
    sql_parser = Lark(fileInfo, start="command", lexer="standard")


def isEndWith(colName, tableColName):
    isEndWithColName = True
    colNameParsed = colName.split(".")
    tableColNameParsed = tableColName.split('.')
    if len(colNameParsed) > len(tableColNameParsed):
        isEndWithColName = False
    for i in range(len(colNameParsed)):
        isEndWithColName &= colNameParsed[-(i + 1)] == tableColNameParsed[-(i + 1)]
    return isEndWithColName


class DataBase:
    prompt = 'DB_2016-15827> '

    def _getInput(self, isTest=False, testFile='input.txt'):
        if isTest:
            return open(testFile).read()
        else:
            output = input(self.prompt)
            while not output.endswith(';'):
                output += '\n' + input()
            return output

    def _putInstruction(self, instruction: str):
        print(self.prompt + instruction)

    def __init__(self):
        self.tables = list()
        dbList = os.listdir('./database')
        # 기존에 존재하는 DB가 있으면 넣어줌.
        dbFileName = None
        for dbFileName in dbList:
            try:
                parsedName = dbFileName.split('.')[0]
                newTable = Table(parsedName, True)

                colInfoRaw = str(newTable.db.get(b'cols'))[3:-2]
                if len(colInfoRaw) != 0:
                    for info in colInfoRaw.split(', '):
                        parsedInfo = info.split('/')
                        colDataType = parsedInfo[0]
                        colName = parsedInfo[1]
                        colLabel = parsedInfo[2]
                        if colLabel == "":
                            colLabel = None
                        colMaxLen = int(parsedInfo[3])
                        colIsNotNull = parsedInfo[4] == "True"
                        newCol = Column(colDataType, colName, colLabel,
                                        colMaxLen, colIsNotNull)
                        newCol.beReferredCnt = int(parsedInfo[5])
                        newTable.addCol(newCol)

                pKeyInfoRaw = str(newTable.db.get(b'pKeys'))[3:-2]
                pKeyInfos = pKeyInfoRaw.split(', ')
                for pKeyInfo in pKeyInfos:
                    newTable.setPrimaryKey(pKeyInfo.split('/')[1])
                newTable.didSetPKeys = True

                # fKey 같은 경우엔 dict 로 저장해서 그대로 넣어줘도 됨.
                fKeyInfo = eval(str(newTable.db.get(b'fKeys'))[2:-1])
                newTable.fKeys = fKeyInfo

                # row 넣어주기
                cursor = newTable.db.cursor()
                while x := cursor.next():
                    if x[0] in [b'cols', b'pKeys', b'fKeys']:
                        continue
                    else:
                        newTable.rows.append(eval(str(x[1])[2:-1]))
                self.tables.append(newTable)
            except:
                # damaged db file error
                self._putInstruction(f"Table '{dbFileName}' has a problem. Check it please.")
        # 참조 설정 추가.
        for table in self.tables:
            for fKeyInfo in table.fKeys:
                refTableName = fKeyInfo['referredTableName']
                refColName = fKeyInfo['referredColName']
                refTable = self.getTable(refTableName)
                if refTable not in table.refTables:
                    table.refTables.append(refTable)
                if table not in refTable.refMeTables:
                    refTable.refMeTables.append(table)

    def getTable(self, tableName):
        for table in self.tables:
            if table.name == tableName:
                return table
        return None

    def _getCompValue(self, query):
        assert query.data == 'comparable_value'
        token = query.children[0]
        if token.type == 'STR':
            return {'type': 'char', 'value': token.value[1:len(token.value) - 1]}
        elif token.type == 'INT':
            return {'type': 'int', 'value': int(token.value)}
        elif token.type == 'DATE':
            return {'type': 'date', 'value': token.value}
        else:
            raise SyntaxError

    def _getOperandValue(self, query, row, tableName):
        assert query.data == 'comp_operand'
        element = query.children[0]
        # table 값이 아니라 constant 일 경우.
        if type(element) == Tree and element.data == 'comparable_value':
            return self._getCompValue(element)
        # table 값이고 table name 이 있을 경우
        elif type(element) == Tree and element.data == 'table_name':
            colName = query.children[1].children[0].value
            if element.data == 'table_name':
                # select 에서 사용될 때이므로 원래 table 명을 column name 에 붙여줌
                originalTableName = element.children[0].value
                colName = f'{originalTableName}.{colName}'
            table = None
            for t in self.tables:
                if t.name == tableName:
                    table = t
                    break
            if table is None:
                raise TableExistenceError("No such table")

            colsWeWant = []
            for tableCol in table.cols:
                if isEndWith(colName, tableCol.name):
                    colsWeWant.append(tableCol)
            if len(colsWeWant) == 0:
                # 다른 테이블에 있는 column name_인지 확인.
                isInOtherTable = False
                for tmpTable in self.tables:
                    for tableCol in tmpTable.cols:
                        if isEndWith(colName, tableCol.name):
                            isInOtherTable = True
                            break
                if isInOtherTable:
                    raise WhereTableNotSpecified("Where clause try to reference tables which are not specified")
                else:
                    # 애초에 없는 테이블일 경우
                    raise WhereColumnNotExist("Where clause try to reference non existing column")
            elif len(colsWeWant) > 1:
                # column_이 하나로 특정되지 않을 경우
                raise WhereAmbiguousReference("Where clause contains ambiguous reference")
            if row == "dummy_data":
                return
            return {'type': colsWeWant[0].dataType, 'value': row[colsWeWant[0].name]}
        # table 값인데 table name 이 없을 경우
        elif type(element) == Tree and element.data == 'column_name':
            table = None
            for t in self.tables:
                if t.name == tableName:
                    table = t
                    break
            if table is None:
                raise TableExistenceError("No such table")
            colNameWeWantToFind = element.children[0].value
            columnsWeWant = []
            # label 먼저 확인
            for tableCol in table.cols:
                if tableCol.label == colNameWeWantToFind:
                    columnsWeWant.append(tableCol)
            if len(columnsWeWant) == 0:
                for tableCol in table.cols:
                    if isEndWith(colNameWeWantToFind, tableCol.name):
                        columnsWeWant.append(tableCol)

            if len(columnsWeWant) > 1:
                raise WhereAmbiguousReference("Where clause contains ambiguous reference")
            elif len(columnsWeWant) == 0:
                # 다른 테이블에 있는 column name_인지 확인.
                for tmpTable in self.tables:
                    for tableCol in tmpTable.cols:
                        if isEndWith(colNameWeWantToFind, tableCol.name):
                            raise WhereTableNotSpecified("Where clause try to reference tables which are not specified")
                # 애초에 없는 테이블일 경우
                raise WhereColumnNotExist("Where clause try to reference non existing column")
            if row == "dummy_data":
                return
            colWeWant = columnsWeWant[0]
            return {'type': colWeWant.dataType, 'value': row[colWeWant.name]}
        else:
            # when null value
            if row == "dummy_data":
                return
            return {'type': 'NULL', 'value': None}

    def _predicate(self, query, row, tableName=None):
        assert query.data == 'predicate'
        if query.children[0].data == 'comparison_predicate':
            operand1 = self._getOperandValue(query.children[0].children[0], row, tableName)
            operand2 = self._getOperandValue(query.children[0].children[2], row, tableName)
            if row == "dummy_data":
                return
            if operand1["value"] is None or operand2["value"] is None:
                return "unknown"
            op = query.children[0].children[1]
            # 타입이 다를 경우 에러 발생
            if operand1['type'] != operand2['type']:
                raise WhereIncomparableError("Where clause try to compare incomparable values")
            if op == '<':
                return operand1['value'] < operand2['value']
            elif op == '>':
                return operand1['value'] > operand2['value']
            elif op == '=':
                return operand1['value'] == operand2['value']
            elif op == '>=':
                return operand1['value'] >= operand2['value']
            elif op == '<=':
                return operand1['value'] <= operand2['value']
            elif op == '!=':
                return operand1['value'] != operand2['value']
            else:
                raise SyntaxError
        elif query.children[0].data == 'null_predicate':
            nullPredicateTree = query.children[0]
            colsWeWant = []
            # table name 이 없을 경우
            if len(list(nullPredicateTree.find_data('table_name'))) == 0 and tableName is None:
                label = nullPredicateTree.children[0].children[0].value
                for table in self.tables:
                    colName = f"{table.name}.{label}"
                    for col in table.cols:
                        if col.name == colName:
                            colsWeWant.append(col)
            # table name 이 있을 경우.
            else:
                colLabel = nullPredicateTree.children[0].children[0].value
                if tableName is None:
                    tableName = nullPredicateTree.children[0].children[0].value
                    colLabel = nullPredicateTree.children[1].children[0].value
                table = None
                for t in self.tables:
                    if t.name == tableName:
                        table = t
                        break
                assert table is not None
                colName = f'{tableName}.{colLabel}'
                for col in table.cols:
                    if col.name == colName:
                        colsWeWant.append(col)
                        break
            if len(colsWeWant) > 1:
                raise WhereAmbiguousReference("Where clause contains ambiguous reference")
            if len(colsWeWant) == 0:
                raise WhereColumnNotExist("Where clause try to reference non existing column")
            if row == "dummy_data":
                return
            colWeWant = colsWeWant[0]
            nullOperationTree = nullPredicateTree.children[1]
            if len(nullOperationTree.children) == 3:
                # is not null
                return row.setdefault(colWeWant.name, None) is not None
            else:
                # is null
                return row.setdefault(colWeWant.name, None) is None

    def _boolExpression(self, query, row, tableName):
        assert query.data == 'boolean_expr'
        boolTerms = []
        for child in query.children:
            if isinstance(child, Tree):
                boolTerms.append(child)
        orVal = False
        for term in boolTerms:
            boolFactors = []
            for child in term.children:
                if isinstance(child, Tree):
                    boolFactors.append(child)
            andVal = True
            for factor in boolFactors:
                boolTest = factor.children[-1]
                tmpTF = True
                # 그냥 join 에서 comp value 학인이므로
                # null value 에 대해서는 항상 return False, 그리고 not condition 일수도 있으므로 null value 가 있다는 걸 알려줘야 함.
                if boolTest.children[0].data == 'predicate':
                    tmpTF = self._predicate(boolTest.children[0], row, tableName)
                elif boolTest.children[0].data == 'parenthesized_boolean_expr':
                    tfInfo = self._parenthesizedBool(boolTest.children[0], row, tableName)
                    tmpTF = tfInfo
                # dummy_data 일 경우 그냥 return
                if row == "dummy_data":
                    return
                # not 일 경우 바꿔줌.
                if isinstance(factor.children[0], Token) and factor.children[0].type == 'NOT':
                    if type(tmpTF) == bool:
                        tmpTF = not tmpTF
                # unknown 처리
                if type(tmpTF) == bool and type(andVal) == bool:
                    andVal = andVal and tmpTF
                else:
                    andVal = 'unknown'
            if type(orVal) == bool and type(andVal) == bool:
                orVal = orVal or andVal
            else:
                if orVal == 'unknown' and andVal == 'unknown':
                    orVal = 'unknown'
                elif orVal == 'unknown':
                    if andVal:
                        orVal = True
                    else:
                        orVal = 'unknown'
                else:
                    if not orVal:
                        orVal = 'unknown'
        return orVal

    def _parenthesizedBool(self, query, row, tableName):
        boolExpression = query.children[1]
        return self._boolExpression(boolExpression, row, tableName)

    def _where(self, query, row, tableName):
        boolExpression = query.children[1]
        return self._boolExpression(boolExpression, row, tableName)

    def getUserInput(self, isTest=False, testFile='input.txt'):
        while True:
            userInput = " "
            while userInput[len(userInput) - 1] != ";":
                userInput += self._getInput(isTest, testFile)
            # case insensitive 이므로 다 Lower 함.
            userInputList = userInput.lower().strip().split(';')
            for i in range(len(userInputList)):
                tmpInput = userInputList[i]
                if tmpInput.strip() == '':
                    continue
                if i is not len(userInputList) - 1:
                    tmpInput += ';'
                try:
                    command = sql_parser.parse(tmpInput)
                    if type(command.children[0]) == Token:
                        if command.children[0].value == 'exit':
                            for table in self.tables:
                                table.closeFile()
                            self.tables = []
                            return
                    query_list = command.children[0]
                    for query in query_list.children:
                        query_type = None
                        try:
                            query_type = query.children[0].data
                        except:
                            raise QueryParsingError('')
                        try:
                            if query_type == "create_table_query":
                                self._createTable(query)
                            elif query_type == "drop_table_query":
                                self._dropTable(query)
                            elif query_type == "desc_query":
                                self._desc(query)
                            elif query_type == "insert_query":
                                self._insert(query)
                            elif query_type == "delete_query":
                                self._delete(query)
                            elif query_type == "select_query":
                                self._select(query)
                            elif query_type == "show_table_query":
                                self._showTables(query)
                            else:
                                raise QueryParsingError('')
                        except Exception as e:
                            if type(e) in myErrors:
                                self._putInstruction(str(e))
                            else:
                                if isTest:
                                    raise e
                                self._putInstruction('Syntax error')
                except Exception as e:
                    if tmpInput.strip() == 'exit;':
                        for table in self.tables:
                            table.closeFile()
                        self.tables = []
                        return
                    else:
                        if isTest:
                            raise e
                        self._putInstruction('Syntax error')

    '''
    query function 여기서부터 시작.
    '''

    def _createTable(self, query):
        try:
            # syntax 확인
            createQuery = query.children[0]
            assert createQuery.data == 'create_table_query' and \
                   createQuery.children[0].type == 'CREATE' and createQuery.children[1].type == 'TABLE' and \
                   createQuery.children[2].data == 'table_name' and createQuery.children[3].data == 'table_element_list'
            tableName = createQuery.children[2].children[0].value
            # TableExistenceError check
            for preExistTable in self.tables:
                if preExistTable.name == tableName:
                    raise TableExistenceError('Create table has failed: table with the same name already exists')
            newTable = Table(tableName, True)
            newTable.initDBFile()
            tableElementList = createQuery.children[3].children
            # table element list 에서 괄호가 제대로 안 되어 있을 경우 syntax error 생성
            assert tableElementList[0].type == 'LP' and tableElementList[len(tableElementList) - 1].type == 'RP'
            tableElementList = tableElementList[1:len(tableElementList) - 1]
            for elementTree in tableElementList:
                # syntax 확인
                assert elementTree.data == 'table_element'
                # 새로운 column 에 해당하는 정보일 경우 column 새로 생성해줌.
                if elementTree.children[0].data == 'column_definition':
                    tokenLen = len(elementTree.children[0].children)
                    elements = elementTree.children[0].children
                    assert tokenLen == 2 or tokenLen == 4
                    assert elements[0].data == 'column_name'
                    assert elements[1].data == 'data_type'
                    isNotNull = False
                    if tokenLen == 4 and elements[2].type == 'NOT' and elements[3].type == 'NULL':
                        isNotNull = True
                    columnNameTree = elementTree.children[0].children[0]
                    columnTypeTree = elementTree.children[0].children[1]
                    columnLabel = columnNameTree.children[0].value
                    columnDataType = columnTypeTree.children[0].value
                    columnMaxLen = 1000
                    # 길이 정보가 있을 경우 추가해줌.
                    if len(columnTypeTree.children) != 1:
                        tokens = columnTypeTree.children
                        if tokens[1].type != 'LP' or tokens[3].type != 'RP' or tokens[2].type != 'INT':
                            raise SyntaxError
                        columnMaxLen = int(tokens[2].value)
                    if columnMaxLen <= 0 and columnDataType == 'char':
                        newTable.drop()
                        raise CharLengthError('Char length should be over 0')
                    newTable.addCol(Column(dataType=columnDataType, name=f'{newTable.name}.{columnLabel}',
                                           label=None, maxLen=columnMaxLen, isNotNull=isNotNull))

                elif elementTree.children[0].data == 'table_constraint_definition':
                    constraintTree = elementTree.children[0]
                    constraintTypeTree = constraintTree.children[0]
                    if constraintTypeTree.data == 'primary_key_constraint':
                        children = constraintTypeTree.children
                        assert children[0].type == 'PRIMARY' and children[1].type == 'KEY'
                        assert children[2].data == 'column_name_list'
                        colNameList = children[2].children
                        # 괄호 제거
                        assert colNameList[0].type == 'LP' and colNameList[len(colNameList) - 1].type == 'RP'
                        colNameList = colNameList[1:len(colNameList) - 1]
                        for colNameTree in colNameList:
                            assert colNameTree.data == 'column_name'
                            colName = colNameTree.children[0].value
                            newTable.setPrimaryKey(f'{newTable.name}.{colName}')
                        newTable.didSetPKeys = True
                    elif constraintTypeTree.data == 'referential_constraint':
                        children = constraintTypeTree.children
                        assert children[0].type == 'FOREIGN' and children[1].type == 'KEY'
                        assert children[2].data == 'column_name_list'
                        colNameListTree = children[2]
                        colNameList = []
                        for colNameTree in colNameListTree.children[1:-1]:
                            colNameList.append(colNameTree.children[0].value)
                        assert children[3].type == 'REFERENCES' and children[4].data == 'table_name' and \
                               children[5].data == 'column_name_list'
                        referredTableName = children[4].children[0].value
                        referredTable = self.getTable(referredTableName)
                        if referredTable is None:
                            newTable.drop()
                            raise ReferenceTableExistenceError(
                                'Create table has failed: foreign key references non existing table'
                            )
                        fKeyInfoList = []
                        referredColNameTreeList = children[5].children[1: len(children[5].children) - 1]  # 괄호 제거
                        for i in range(len(colNameList)):
                            colName = colNameList[i]
                            referredColName = referredColNameTreeList[i].children[0].value
                            fKeyInfoList.append({'colName': f'{newTable.name}.{colName}',
                                                 'referredColName': f'{referredTableName}.{referredColName}'})
                        newTable.setForeignKey(referredTable, fKeyInfoList)
                else:
                    raise SyntaxError
            self.tables.append(newTable)
            self._putInstruction("'CREATE TABLE' requested")
            return True
        except Exception as e:
            raise e

    def _dropTable(self, query):
        try:
            dropTableQuery = query.children[0]
            assert dropTableQuery.data == 'drop_table_query'
            assert dropTableQuery.children[0].type == 'DROP' and dropTableQuery.children[1].type == 'TABLE'
            assert dropTableQuery.children[2].data == 'table_name'
            tableName = dropTableQuery.children[2].children[0].value
            idx = -1
            for i in range(len(self.tables)):
                if self.tables[i].name == tableName:
                    idx = i
                    break
            if idx == -1:
                raise NoSuchTable('No such table')
            table = self.tables[idx]
            isReferred = False
            for col in table.cols:
                isReferred |= col.beReferredCnt != 0
            if not isReferred:
                table.drop()
                self.tables.pop(idx)
                self._putInstruction(f"'{tableName}' table is dropped")
                return True
            else:
                raise DropReferencedTableError(
                    f"Drop table has failed: '{tableName}' is referenced by other table"
                )

        except Exception as e:
            raise e

    def _desc(self, query):
        try:
            descQuery = query.children[0]
            children = descQuery.children
            assert descQuery.data == 'desc_query'
            assert children[0].type == 'DESC' and children[1].data == 'table_name'
            tableName = children[1].children[0].value
            for table in self.tables:
                if table.name == tableName:
                    table.showInfo()
                    return True
            raise NoSuchTable('No such table')
        except Exception as e:
            raise e

    def _insert(self, query):
        try:
            insertQuery = query.children[0]
            assert insertQuery.data == 'insert_query'
            assert insertQuery.children[0].type == 'INSERT'
            assert insertQuery.children[1].type == 'INTO'
            assert insertQuery.children[2].data == 'table_name'
            assert insertQuery.children[3].data == 'insert_columns_and_sources'
            tableNameTree = insertQuery.children[2]
            sourceTree = insertQuery.children[3]

            tableName = tableNameTree.children[0].value
            table = None
            for t in self.tables:
                if t.name == tableName:
                    table = t
                    break
            if table is None:
                raise NoSuchTable("No such table")

            colData = []
            # column name list 가 있을 때
            if len(sourceTree.children) == 2:
                # 타입 확인.
                assert sourceTree.children[0].data == 'column_name_list'
                assert sourceTree.children[1].data == 'value_list'
                colTokenList = sourceTree.children[0].children
                valTokenList = sourceTree.children[1].children
                # 괄호 및 'value' 확인
                assert colTokenList[0].type == 'LP' and colTokenList[len(colTokenList) - 1].type == 'RP'
                assert valTokenList[0].type == 'VALUES' and \
                       valTokenList[1].type == 'LP' and valTokenList[len(valTokenList) - 1].type == 'RP'
                colTreeList = colTokenList[1:len(colTokenList) - 1]
                valTreeList = valTokenList[2:len(valTokenList) - 1]
                # column name 개수, column value 개수가 다르면 raise error
                if len(colTreeList) != len(valTreeList):
                    raise InsertTypeMismatchError("Insertion has failed: Types are not matched")
                for i in range(len(colTreeList)):
                    colTree = colTreeList[i]
                    valTree = valTreeList[i]
                    assert colTree.data == 'column_name' and valTree.data == 'value'
                    assert colTree.children[0].type == 'IDENTIFIER'
                    data = dict()
                    data['column_name'] = f'{table.name}.{colTree.children[0].value}'
                    if type(valTree.children[0]) is Tree:
                        # comparable value 일 경우
                        data['type'] = valTree.children[0].children[0].type
                        data['value'] = valTree.children[0].children[0].value
                    else:
                        # Null 일 경우
                        data['type'] = valTree.children[0].type
                        data['value'] = valTree.children[0].value
                    if data['type'] == 'INT':
                        data['value'] = int(data['value'])
                    if data['type'] == 'STR':
                        # string 일 경우 따옴표 없에줌.
                        assert data['value'][0] == data['value'][len(data['value'][0]) - 1] and \
                               data['value'][0] in ['\'', '"']
                        data['value'] = data['value'][1:len(data['value']) - 1]
                    if data['type'] == 'NULL':
                        data['value'] = None
                    colData.append(data)
            # value 만 있을 때
            elif len(sourceTree.children) == 1:
                assert sourceTree.children[0].data == 'value_list'
                valTokenList = sourceTree.children[0].children
                assert valTokenList[0].type == 'VALUES' and \
                       valTokenList[1].type == 'LP' and valTokenList[len(valTokenList) - 1].type == 'RP'
                valTreeList = valTokenList[2:len(valTokenList) - 1]
                # 개수가 다르면 raise error
                if len(table.cols) != len(valTreeList):
                    raise InsertTypeMismatchError("Insertion has failed: Types are not matched")
                for i in range(len(table.cols)):
                    valTree = valTreeList[i]
                    assert valTree.data == 'value'
                    data = dict()
                    data['column_name'] = table.cols[i].name
                    if isinstance(valTree.children[0], Tree):
                        data['type'] = valTree.children[0].children[0].type
                        data['value'] = valTree.children[0].children[0].value
                    elif isinstance(valTree.children[0], Token):
                        assert valTree.children[0].type == 'NULL'
                        data['type'] = valTree.children[0].type
                        data['value'] = None
                    if data['type'] == 'INT':
                        data['value'] = int(data['value'])
                    if data['type'] == 'STR':
                        # string 일 경우 따옴표 없에줌.
                        assert data['value'][0] == data['value'][len(data['value'][0]) - 1] and \
                               data['value'][0] in ['\'', '"']
                        data['value'] = data['value'][1:len(data['value']) - 1]
                    if data['type'] == 'NULL':
                        data['value'] = None
                    colData.append(data)
            else:
                raise SyntaxError
            # insert 하려는 data 개수와 table column 개수가 다르면 에러
            if len(colData) != len(table.cols):
                raise InsertTypeMismatchError("Insertion has failed: Types are not matched")
            rowInfo = dict()
            for data in colData:
                idx = -1
                for i in range(len(table.cols)):
                    c = table.cols[i]
                    if c.name == data['column_name']:
                        idx = i
                        break
                if idx == -1:
                    raise InsertColumnExistenceError(
                        f"Insertion has failed: '{data['column_name'][len(tableName) + 1:]}' does not exist"
                    )
                col = table.cols[idx]
                if data['type'] == 'NULL':
                    # error 판단 부분 Table class 에서 처리하는 걸로 바뀜.
                    rowInfo[col.name] = {'type': col.dataType, 'value': None}
                else:
                    def converter(dType):
                        if dType == 'INT' or dType == 'int':
                            return 'int'
                        elif dType == 'STR' or dType == 'char':
                            return 'char'
                        elif dType == 'date' or dType == 'DATE':
                            return 'date'
                        else:
                            raise SyntaxError

                    # error 판단 부분 Table class 에서 처리하는 걸로 바뀜.
                    rowInfo[col.name] = {'type': converter(data['type']), 'value': data['value']}
            table.addRow(rowInfo)
            self._putInstruction("'INSERT' requested")
            return True
        except Exception as e:
            raise e

    def _delete(self, query):
        try:
            deleteQuery = query.children[0]
            assert deleteQuery.data == 'delete_query' and deleteQuery.children[0].type == 'DELETE' and \
                   deleteQuery.children[1].type == 'FROM' and deleteQuery.children[2].data == 'table_name'
            assert len(deleteQuery.children) == 3 or len(deleteQuery.children) == 4
            tableName = deleteQuery.children[2].children[0].value
            table = None
            for t in self.tables:
                if t.name == tableName:
                    table = t
                    break
            if table is None:
                raise NoSuchTable("No such table")
            # 모든 row 를 삭제할 때
            cntDelete = 0
            cntNotDelete = 0
            maybeDeleted = []
            if len(deleteQuery.children) == 3:
                for row in table.rows:
                    maybeDeleted.append(row)
            # where 절이 있을 경우
            elif len(deleteQuery.children) == 4:
                whereClauseTree = deleteQuery.children[3]
                # row_가 하나도 없을 때 WhereColumnNotExist, WhereAmbiguousReference 체크하기 위해 필요함.
                self._where(whereClauseTree, "dummy_data", tableName)
                for row in table.rows:
                    if self._where(whereClauseTree, row, tableName) == True:
                        maybeDeleted.append(row)
            for row in maybeDeleted:
                if table.deleteRow(row):
                    cntDelete += 1
                else:
                    cntNotDelete += 1
            self._putInstruction(f"{cntDelete} row(s) are deleted")
            if cntNotDelete != 0:
                self._putInstruction(f"{cntNotDelete} row(s) are not deleted due to referential integrity")
            return True
        except Exception as e:
            raise e

    def _join_helper(self, table1: Table, table2: Table, tableName: str):
        """
        테이블 가명은 이미 다 적용된 상태.
        :return: Table
        """
        if tableName is None:
            table1Name = table1.name
            if table1Name.startswith("_joined_"):
                table1Name = table1Name[len('_joined_'):]
            table2Name = table2.name
            if table2Name.startswith("_joined_"):
                table2Name = table2Name[len('_joined_'):]
            tableName = f'_joined_{table1Name},{table2Name}'
        newTable = Table(tableName, False)
        for col in table1.cols:
            colName = col.name
            if table1.name.startswith('_joined_'):
                colName = colName[len(table1.name)+1:]
            copiedCol = col.copy(name=f'{tableName}.{colName}')

            newTable.addCol(copiedCol)
        for col in table2.cols:
            colName = col.name
            if table2.name.startswith('_joined_'):
                colName = colName[len(table2.name)+1:]
            copiedCol = col.copy(name=f'{tableName}.{colName}')
            newTable.addCol(copiedCol)

        # 새 테이블에 row 넣어줌.
        for i1 in range(len(table1.rows)):
            row1 = table1.rows[i1]
            for i2 in range(len(table2.rows)):
                row2 = table2.rows[i2]
                newRow = dict()
                for row1Key in row1:
                    if row1Key != '_isReferred':
                        tmpRowKey = row1Key
                        if table1.name.startswith('_joined_'):
                            tmpRowKey = row1Key[len(table1.name)+1:]
                        newRow[f'{tableName}.{tmpRowKey}'] = row1[row1Key]
                for row2Key in row2:
                    if row2Key != '_isReferred':
                        tmpRowKey = row2Key
                        if table2.name.startswith('_joined_'):
                            tmpRowKey = row2Key[len(table2.name)+1:]
                        newRow[f'{tableName}.{tmpRowKey}'] = row2[row2Key]
                newTable.rows.append(newRow)
        return newTable

    def _select(self, query):
        def getTableList(inputQ):
            # 1. column label 정보를 구함.
            selectedColTrees = list(inputQ.find_data('selected_column'))
            colLabelInfos = []
            for colTree in selectedColTrees:
                tableNameList = []
                colNameList = []
                for child in colTree.children:
                    if type(child) is Tree:
                        if child.data == "column_name":
                            colNameList.append(child.children[0].value)
                        elif child.data == "table_name":
                            tableNameList.append(child.children[0].value)
                labelInfo = dict()
                labelInfo["count"] = 0
                if len(tableNameList) == 1:
                    labelInfo["tableName"] = tableNameList[0]
                else:
                    labelInfo["tableName"] = None
                labelInfo["colName"] = colNameList[0]
                if len(colNameList) == 2:
                    labelInfo["label"] = colNameList[1]
                else:
                    labelInfo["label"] = colNameList[0]  # label 이 없는 경우 원래 이름이랑 똑같이 label 설정함.
                colLabelInfos.append(labelInfo)

            # 2. select 하고자 하는 table 들의 list를 구함. 가명을 쓰고 있으면 table name 가명으로 바꿔서 집어넣음.
            referredTables = []
            referredTableTrees = list(inputQ.find_data('referred_table'))
            for referred in referredTableTrees:
                tableName = referred.children[0].children[0].value
                table = self.getTable(tableName)
                if table is None:
                    raise SelectTableExistenceError(f"Selection has failed: '{tableName}' does not exist")
                if len(referred.children) == 3:
                    alias = referred.children[2].children[0].value
                    table = table.copy(alias)

                for col in table.cols:
                    for labelInfo in colLabelInfos:
                        label = labelInfo["label"]

                        tableName = labelInfo["tableName"]
                        if tableName == table.name:
                            colName = labelInfo["colName"]
                            if f"{tableName}.{colName}" == col.name:
                                col.label = label
                                labelInfo["count"] += 1
                        elif tableName is None:
                            colName = labelInfo["colName"]
                            if colName == col.name[len(table.name) + 1:]:
                                col.label = label
                                labelInfo["count"] += 1

                referredTables.append(table)

            for labelInfo in colLabelInfos:
                if labelInfo["count"] != 1:
                    raise SelectColumnResolveError(f"Selection has failed: fail to resolve '{labelInfo['colName']}'")

            return referredTables

        try:
            selectQuery = query.children[0]
            assert selectQuery.data == 'select_query' and selectQuery.children[0].type == 'SELECT'
            tableList = getTableList(selectQuery)
            assert len(tableList) > 0

            tableWeWant = tableList[0]
            tmpTables = []
            for table in tableList:
                if table not in self.tables:
                    tmpTables.append(table)
            for i in range(1, len(tableList)):
                tableWeWant = self._join_helper(tableWeWant, tableList[i], None)
                # 모든 항목에 대해 다 join 함.
                # join 중간에 나오는 임시 테이블들은 다 없에야 하므로 따로 저장.
                tmpTables.append(tableWeWant)
            # where 절 처리를 위해 잠시 tables 에 넣어줌
            if tableWeWant not in self.tables:
                self.tables.append(tableWeWant)

            # 어떤 column 을 select 할 것이냐
            selectList = selectQuery.children[1].children
            selectedColumns = []
            for colTree in selectList:
                colTableName = None
                colTNList = list(colTree.find_data("table_name"))
                colCNList = list(colTree.find_data("column_name"))
                if len(colTNList) == 1:
                    colTableName = colTNList[0].children[0].value
                colColName = colCNList[0].children[0].value

                expectedColName = colColName
                if colTableName is not None:
                    expectedColName = f"{colTableName}.{colColName}"
                candidates = []
                for col in tableWeWant.cols:
                    if isEndWith(colName=expectedColName, tableColName=col.name):
                        candidates.append(col)
                if len(candidates) != 1:
                    raise SelectColumnResolveError(f"Selection has failed: fail to resolve '{expectedColName}'")
                selectedColumns.append(candidates[0])
            # when select *
            if len(selectedColumns) == 0:
                for col in tableWeWant.cols:
                    selectedColumns.append(col)

            tableExpressionTree = selectQuery.children[2]
            whereClauses = list(tableExpressionTree.find_data('where_clause'))
            rowsWeWant = []
            if len(whereClauses) == 0:
                # where 조건이 없으면 전체를  output 으로 내보냄
                for row in tableWeWant.rows:
                    rowsWeWant.append(row)
            else:
                whereClause = whereClauses[0]
                # row_가 하나도 없을 때 WhereColumnNotExist, WhereAmbiguousReference 체크하기 위해 필요함.
                self._where(whereClause, "dummy_data", tableWeWant.name)
                for row in tableWeWant.rows:
                    if self._where(whereClause, row, tableWeWant.name) == True:
                        rowsWeWant.append(row)

            # 출력 양식을 위한 함수들.
            def getTabSize(n: int):
                minTabSize = (n - 1) // 4
                return max((minTabSize + 1) * 4, 16)

            def printRow(row: dict, tabSizeInfo: list):
                logStr = '|'
                for i in range(len(selectedColumns)):
                    tabSize = tabSizeInfo[i]
                    colName = selectedColumns[i].name
                    data = row[colName]
                    dataStr = str(data)
                    if data is None:
                        dataStr = "null"
                    logStr += f' {dataStr}{" " * (tabSize - len(dataStr))} |'
                print(logStr)

            maxLenInfo = []
            for col in selectedColumns:
                label = col.label
                if label is None:
                    label = col.name[len(tableWeWant.name)+1:]
                maxLenInfo.append(len(label))
            for row in rowsWeWant:
                for i in range(len(selectedColumns)):
                    col = selectedColumns[i]
                    dataLen = len(str(row[col.name]))
                    if maxLenInfo[i] < dataLen:
                        maxLenInfo[i] = dataLen
            tabSizeInfo = []
            for n in maxLenInfo:
                tabSizeInfo.append(getTabSize(n))
            border = '+'
            colNameLogStr = '|'
            for i in range(len(selectedColumns)):
                tabSize = tabSizeInfo[i]
                colLabel = selectedColumns[i].label
                if colLabel is None:
                    colLabel = selectedColumns[i].name[len(tableWeWant.name)+1:]
                border += f"-{'-' * tabSize}-+"
                colNameLogStr += f' {colLabel}{" " * (tabSize - len(colLabel))} |'
            print(border)
            print(colNameLogStr)
            print(border)
            for row in rowsWeWant:
                printRow(row, tabSizeInfo)
            print(border)
            # 중간에 join 되었던 table 제거.
            for table in tmpTables:
                table.drop()
                if table in self.tables:
                    self.tables.pop(self.tables.index(table))
            return True
        except Exception as e:
            raise e

    def _showTables(self, query):
        try:
            showTableQuery = query.children[0]
            assert showTableQuery.children[0].type == 'SHOW' and showTableQuery.children[1].type == 'TABLES'
            print('----------------')
            for table in self.tables:
                print(table.name)
            print('----------------')
            return True
        except Exception as e:
            raise e


if __name__ == '__main__':
    DB = DataBase()
    DB.getUserInput(True)
