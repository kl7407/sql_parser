from lark import *
from Table import *

with open('grammar.lark') as file:
    fileInfo = file.read()
    sql_parser = Lark(fileInfo, start="command", lexer="standard")


def isAlphabet(letter: str):
    if len(letter) != 1:
        return False
    alphabets = 'abcdefghijklmnopqrstuvwxyz_'
    return letter.lower() in alphabets


class DataBase:
    prompt = 'DB_2016-15827> '

    def _getInput(self, isTest=False, testFile='input.txt'):
        if isTest:
            return open(testFile).read()
        else:
            return input(self.prompt)

    def _putInstruction(self, instruction: str):
        print(self.prompt + instruction)

    def __init__(self):
        self.tables = list()

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

    def _getOperandValue(self, query, row, tableName=None):
        assert query.data == 'comp_operand'
        element = query.children[0]
        if element.data == 'comparable_value':
            return self._getCompValue(element)
        # table name 이 있을 경우
        elif element.data == 'table_name' or tableName is not None:
            colName = element.children[0].value
            if element.data == 'table_name':
                tableName = element.children[0].value
                colName = query.children[1].children[0].value
            table = None
            for t in self.tables:
                if t.name == tableName:
                    table = t
                    break
            assert table is not None
            colWeWant = None
            for tableCol in table.cols:
                if tableCol.name == colName:
                    colWeWant = tableCol
            assert colWeWant is not None
            return {'type': colWeWant.dataType, 'value': row[colWeWant.name]}
        # table name 이 없을 경우
        elif element.data == 'column_name':
            columnsWeWant = []
            for table in self.tables:
                for tableCol in table.cols:
                    if tableCol.name == element.children[0].value:
                        columnsWeWant.append(tableCol)
            assert len(columnsWeWant) == 1
            colWeWant = columnsWeWant[0]
            return {'type': colWeWant.dataType, 'value': row[colWeWant.name]}

    def _predicate(self, query, row, tableName=None):
        assert query.data == 'predicate'
        if query.children[0].data == 'comparison_predicate':
            operand1 = self._getOperandValue(query.children[0].children[0], row, tableName)
            operand2 = self._getOperandValue(query.children[0].children[2], row, tableName)
            op = query.children[0].children[1]
            assert operand1['type'] == operand2['type']
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
                colName = nullPredicateTree.children[0].children[0].value
                for table in self.tables:
                    for col in table.cols:
                        if col.name == colName:
                            colsWeWant.append(col)
            # table name 이 있을 경우.
            else:
                colName = nullPredicateTree.children[0].children[0].value
                if tableName is None:
                    tableName = nullPredicateTree.children[0].children[0].value
                    colName = nullPredicateTree.children[1].children[0].value
                table = None
                for t in self.tables:
                    if t.name == tableName:
                        table = t
                        break
                assert table is not None
                for col in table.cols:
                    if col.name == colName:
                        colsWeWant.append(col)
                        break
            assert len(colsWeWant) == 1
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
                if boolTest.children[0].data == 'predicate':
                    tmpTF = self._predicate(boolTest.children[0], row, tableName)
                elif boolTest.children[0].data == 'parenthesized_boolean_expr':
                    tmpTF = self._parenthesizedBool(boolTest.children[0], row, tableName)
                if isinstance(factor.children[0], Token) and factor.children[0].type == 'NOT':
                    tmpTF = not tmpTF
                andVal = andVal and tmpTF
            orVal = orVal or andVal
        return orVal

    def _parenthesizedBool(self, query, row, tableName):
        boolExpression = query.children[1]
        return self._boolExpression(boolExpression, row, tableName)

    def _where(self, query, row, tableName=None):
        boolExpression = query.children[1]
        return self._boolExpression(boolExpression, row, tableName)

    def getUserInput(self, isTest=False, testFile='input.txt'):
        userInput = " "
        while userInput[len(userInput) - 1] != ";":
            userInput += self._getInput(isTest, testFile)
        try:
            command = sql_parser.parse(userInput)
            query_list = command.children[0]
            for query in query_list.children:
                query_type = query.children[0].data
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
                    raise SyntaxError
        except:
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
            newTable = Table(tableName)
            tableElementList = createQuery.children[3].children
            # table element list 에서 괄호가 제대로 안 되어 있을 경우 syntax error 생성
            assert tableElementList[0].type == 'LP' and tableElementList[len(tableElementList)-1].type == 'RP'
            tableElementList = tableElementList[1:len(tableElementList)-1]
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
                    columnName = columnNameTree.children[0].value
                    columnDataType = columnTypeTree.children[0].value
                    columnMaxLen = 1000
                    # 길이 정보가 있을 경우 추가해줌.
                    if len(columnTypeTree.children) != 1:
                        tokens = columnTypeTree.children
                        if tokens[1].type != 'LP' or tokens[3].type != 'RP' or tokens[2].type != 'INT':
                            raise SyntaxError
                        columnMaxLen = int(tokens[2].value)
                    newTable.addCol(Column(columnDataType, columnName, columnMaxLen, isNotNull))

                elif elementTree.children[0].data == 'table_constraint_definition':
                    constraintTree = elementTree.children[0]
                    constraintTypeTree = constraintTree.children[0]
                    if constraintTypeTree.data == 'primary_key_constraint':
                        children = constraintTypeTree.children
                        assert children[0].type == 'PRIMARY' and children[1].type == 'KEY'
                        assert children[2].data == 'column_name_list'
                        colNameList = children[2].children
                        # 괄호 제거
                        assert colNameList[0].type == 'LP' and colNameList[len(colNameList)-1].type == 'RP'
                        colNameList = colNameList[1:len(colNameList)-1]
                        for colNameTree in colNameList:
                            assert colNameTree.data == 'column_name'
                            colName = colNameTree.children[0].value
                            newTable.setPrimaryKey(colName)
                    elif constraintTypeTree.data == 'referential_constraint':
                        children = constraintTypeTree.children
                        assert children[0].type == 'FOREIGN' and children[1].type == 'KEY'
                        assert children[2].data == 'column_name_list'
                        colNameListTree = children[2]
                        colNameList = colNameListTree.children[1].children
                        assert children[3].type == 'REFERENCES' and children[4].data == 'table_name' and \
                               children[5].data == 'column_name_list'
                        referredTableName = children[4].children[0].value
                        referredColNameTreeList = children[5].children[1: len(children[5].children) - 1]  # 괄호 제거
                        for i in range(len(colNameList)):
                            colNameTree = colNameList[i]
                            colName = colNameTree.value
                            referredColName = referredColNameTreeList[i].children[0].value
                            newTable.setForeignKey(colName, referredTableName, referredColName)
                else:
                    raise SyntaxError
            self.tables.append(newTable)
            self._putInstruction("'CREATE TABLE' requested")
            return True
        except:
            self._putInstruction('Syntax error')
            return False

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
                raise SyntaxError
            self.tables.pop(idx)
            self._putInstruction("'DROP TABLE' requested")
            return True
        except:
            self._putInstruction('Syntax error')
            return False

    def _desc(self, query):
        try:
            descQuery = query.children[0]
            children = descQuery.children
            assert descQuery.data == 'desc_query'
            assert children[0].type == 'DESC' and children[1].data == 'table_name'
            # tableName = children[1].children[0].value
            self._putInstruction("'DESC' requested")
            return True
        except:
            self._putInstruction('Syntax error')
            return False

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
                raise SyntaxError

            colData = []
            # column name list 가 있을 때
            if len(sourceTree.children) == 2:
                # 타입 확인.
                assert sourceTree.children[0].data == 'column_name_list'
                assert sourceTree.children[1].data == 'value_list'
                colTokenList = sourceTree.children[0].children
                valTokenList = sourceTree.children[1].children
                # 괄호 및 'value' 확인
                assert colTokenList[0].type == 'LP' and colTokenList[len(colTokenList)-1].type == 'RP'
                assert valTokenList[0].type == 'VALUES' and \
                       valTokenList[1].type == 'LP' and valTokenList[len(valTokenList)-1].type == 'RP'
                colTreeList = colTokenList[1:len(colTokenList)-1]
                valTreeList = valTokenList[2:len(valTokenList)-1]
                assert len(colTreeList) == len(valTreeList)
                for i in range(len(colTreeList)):
                    colTree = colTreeList[i]
                    valTree = valTreeList[i]
                    assert colTree.data == 'column_name' and valTree.data == 'value'
                    assert colTree.children[0].type == 'IDENTIFIER'
                    data = dict()
                    data['column_name'] = colTree.children[0].value
                    data['type'] = valTree.children[0].children[0].type
                    data['value'] = valTree.children[0].children[0].value
                    if data['type'] == 'INT':
                        data['value'] = int(data['value'])
                    if data['type'] == 'STR':
                        # string 일 경우 따옴표 없에줌.
                        assert data['value'][0] == data['value'][len(data['value'][0])-1] and \
                               data['value'][0] in ['\'', '"']
                        data['value'] = data['value'][1:len(data['value'])-1]
                    if data['type'] == 'NULL':
                        data['value'] = None
                    colData.append(data)
            # value 만 있을 때
            elif len(sourceTree.children) == 1:
                assert sourceTree.children[0].data == 'value_list'
                valTokenList = sourceTree.children[0].children
                assert valTokenList[0].type == 'VALUES' and \
                       valTokenList[1].type == 'LP' and valTokenList[len(valTokenList)-1].type == 'RP'
                valTreeList = valTokenList[2:len(valTokenList) - 1]
                assert len(table.cols) == len(valTreeList)
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
            row = dict()
            for data in colData:
                idx = -1
                for i in range(len(table.cols)):
                    c = table.cols[i]
                    if c.name == data['column_name']:
                        idx = i
                        break
                assert idx != -1
                col = table.cols[idx]
                if data['type'] == 'NULL':
                    assert not col.isNotNull
                    row[col.name] = {'type': col.dataType, 'value': None}
                else:
                    assert col.dataType == 'int' and data['type'] == 'INT' or \
                           col.dataType == 'char' and data['type'] == 'STR' or \
                           col.dataType == 'date' and data['type'] == 'DATE'
                    row[col.name] = {'type': col.dataType, 'value': data['value']}
            table.addRow(row)
            self._putInstruction("'INSERT' requested")
            return True
        except:
            self._putInstruction('Syntax error')
            return False

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
            assert table is not None
            # 모든 row 를 삭제할 때
            if len(deleteQuery.children) == 3:
                table.rows = []
            # where 절이 있을 경우
            elif len(deleteQuery.children) == 4:
                whereClauseTree = deleteQuery.children[3]
                shouldBeDeleted = []
                for row in table.rows:
                    if self._where(whereClauseTree, row, tableName):
                        shouldBeDeleted.append(row)
                for row in shouldBeDeleted:
                    table.rows.remove(row)
            self._putInstruction("'DELETE' requested")
            return True
        except:
            self._putInstruction('Syntax error')
            return False

    def _join_helper(self, table1: Table, table2: Table, tableName: str):
        def isIn(colName, t):
            for col in t.cols:
                if col.name.split('.')[-1] == colName and len(colName.split('.')) == 1:
                    return True
            return False

        def getTableName(table, colName):
            originalTable = None
            for oTable in table.originalTables:
                found = False
                for oCol in oTable.cols:
                    if oCol.name == colName:
                        originalTable = oTable
                        found = True
                        break
                if found:
                    break
            assert originalTable is not None
            return '{}.{}'.format(originalTable.name, colName)
        """
        테이블 가명은 이미 다 적용된 상태.

        :param asInfo: [{ 'tableName': str, 'colName': str, 'newName': str}, ...]
        :return: Table
        """
        if tableName is None:
            tableName = 'joined_{}_{}'.format(table1.name, table2.name)
        newTable = Table(tableName)
        # table1, table2 에 같은 이름의 column이 있으면 앞에 테이블 이름 추가해서 넣어줌.
        for col in table1.cols:
            colName = col.name
            if isIn(colName, table2):
                if len(table1.originalTables) == 0:
                    colName = '{}.{}'.format(table1.name, colName)
                else:
                    colName = getTableName(table1, colName)
            newTable.addCol(col.copy(colName))
        for col in table2.cols:
            colName = col.name
            if isIn(colName, table1):
                if len(table2.originalTables) == 0:
                    colName = '{}.{}'.format(table2.name, colName)
                else:
                    colName = getTableName(table2, colName)
            newTable.addCol(col.copy(colName))

        # 새 테이블에 row 넣어줌.
        for i1 in range(len(table1.rows)):
            row1 = table1.rows[i1]
            for i2 in range(len(table2.rows)):
                row2 = table2.rows[i2]
                newRow = dict()
                for row1Key in row1:
                    if isIn(row1Key, table2):
                        if len(table1.originalTables) == 0:
                            newRow[table1.name + '.' + row1Key] = row1[row1Key]
                        else:
                            newRow[getTableName(table1, row1Key)] = row1[row1Key]
                    else:
                        newRow[row1Key] = row1[row1Key]
                for row2Key in row2:
                    if isIn(row2Key, table1):
                        if len(table2.originalTables) == 0:
                            newRow[table2.name + '.' + row2Key] = row2[row2Key]
                        else:
                            newRow[getTableName(table2, row2Key)] = row2[row2Key]
                    else:
                        newRow[row2Key] = row2[row2Key]
                newTable.rows.append(newRow)
        newTable.addOriginalTable(table1)
        newTable.addOriginalTable(table2)

        return newTable

    def _select_helper(self, table: Table, whereClause, asInfo):
        pass

    def _select(self, query):
        def getTableList(inputQ):
            # select 하고자 하는 table 들의 list를 구함. 가명을 쓰고 있으면 table name 가명으로 바꿔서 집어넣음.
            referredTables = []
            referredTableTrees = list(inputQ.find_data('referred_table'))
            for referred in referredTableTrees:
                tableName = referred.children[0].children[0].value
                table = self.getTable(tableName)
                if len(referred.children) == 3:
                    alias = referred.children[2].children[0].value
                    table = table.copy(alias)
                referredTables.append(table)
            return referredTables
        try:
            selectQuery = query.children[0]
            assert selectQuery.data == 'select_query' and selectQuery.children[0].type == 'SELECT'
            tableList = getTableList(selectQuery)
            assert len(tableList) > 0

            tableWeWant = tableList[0]
            for i in range(1, len(tableList)):
                tableWeWant = self._join_helper(tableWeWant, tableList[i], None)


            selectList = selectQuery.children[1].children
            selectedCols = []
            tableExpressionTree = selectQuery.children[2]
            fromClause = list(tableExpressionTree.find_data('from_clause'))[0]
            whereClauses = list(tableExpressionTree.find_data('where_clause'))
            tableReferenceList = list(fromClause.children[1].find_data('referred_table'))
            if len(whereClauses) == 0:
                pass
            else:
                pass
            pass
            return True
        except:
            self._putInstruction('Syntax error')
            return False

    def _showTables(self, query):
        try:
            showTableQuery = query.children[0]
            assert showTableQuery.children[0] == 'SHOW' and showTableQuery.children[1] == 'TABLES'
            return True
        except:
            self._putInstruction('Syntax error')
            return False


DB = DataBase()
DB.getUserInput(True)
