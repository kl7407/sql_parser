from lark import *

with open('grammar.lark') as file:
    fileInfo = file.read()
    sql_parser = Lark(fileInfo, start="command", lexer="standard")


def isAlphabet(letter: str):
    if len(letter) != 1:
        return False
    alphabets = 'abcdefghijklmnopqrstuvwxyz_'
    return letter.lower() in alphabets


def parseParenthesis(tree: Tree):
    tableElementList = tree.children
    isAlright = True
    fLen = len(tableElementList)
    try:
        if not isinstance(tableElementList[0], Tree):
            hasLP = tableElementList[0].type == 'LP'
            hasRP = tableElementList[fLen - 1].type == 'RP'
            if not hasLP or not hasRP:
                isAlright = False
            # 괄호가 있으면 괄호 제거하고 parsing 진행.
            tableElementList = tableElementList[1: fLen - 1]
    except:
        isAlright = False
    return isAlright, tableElementList


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
                    self._createTable(query.children[0])
                elif query_type == "drop_table_query":
                    self._dropTable(query)
                elif query_type == "desc_query":
                    self._desc(query)
                elif query_type == "insert_query":
                    print("INSERT_QUERY")
                elif query_type == "delete_query":
                    print("DELETE_QUERY")
                elif query_type == "select_query":
                    print("SELECT_QUERY")
                elif query_type == "show_table_query":
                    print("SHOW_TABLE_QUERY")
                else:
                    raise SyntaxError
        except Exception:
            self._putInstruction('Syntax error')

    def _createTable(self, query):
        self._putInstruction("CREATE_TABLE_QUERY")
        try:
            tableNameToken = list(query.find_data("table_name"))[0].children[0]
            assert tableNameToken.type == 'IDENTIFIER'
            tableName = tableNameToken.value
            newTable = Table(tableName)
            tableElementListTree = list(query.find_data('table_element_list'))[0]
            tableElementList = tableElementListTree.children
            # table element list 에서 괄호가 제대로 안 되어 있을 경우 syntax error 생성
            if tableElementList[0].type != 'LP' or tableElementList[len(tableElementList)-1].type != 'RP':
                raise SyntaxError
            for elementTree in list(query.find_data('table_element')):
                # 새로운 column에 해당하는 정보일 경우 column 새로 생성해줌.
                if elementTree.children[0].data == 'column_definition':
                    columnNameTree = list(elementTree.find_data('column_name'))[0]
                    columnTypeTree = list(elementTree.find_data('data_type'))[0]
                    columnInfo = dict()
                    columnInfo['name'] = columnNameTree.children[0].value
                    columnInfo['type'] = columnTypeTree.children[0].value
                    # 길이 정보가 있을 경우 추가해줌.
                    if len(columnTypeTree.children) != 1:
                        tokens = columnTypeTree.children
                        if tokens[1].type != 'LP' or tokens[3].type != 'RP' or tokens[2].type != 'INT':
                            raise SyntaxError
                        columnInfo['maxLength'] = int(tokens[2].value)
                    newTable.addCol(columnInfo)

                elif elementTree.children[0].data == 'table_constraint_definition':
                    constraintTree = elementTree.children[0]
                    constraintTypeTree = constraintTree.children[0]
                    if constraintTypeTree.data == 'primary_key_constraint':
                        children = constraintTypeTree.children
                        if children[0].type != 'PRIMARY' or children[1].type != 'KEY':
                            raise SyntaxError
                        assert children[2].data == 'column_name_list'
                        colNameList = children[2].children
                        if colNameList[0].type != 'LP' or colNameList[len(colNameList)-1].type != 'RP':
                            raise SyntaxError
                        colNameList = colNameList[1:len(colNameList)-1]
                        for colNameTree in colNameList:
                            assert colNameTree.data == 'column_name'
                            colName = colNameTree.children[0].value
                            if newTable.setPrimaryKey(colName) != 1:
                                raise SyntaxError
                    elif constraintTypeTree.data == 'referential_constraint':
                        children = constraintTypeTree.children
                        if children[0].type != 'FOREIGN' or children[1].type != 'KEY':
                            raise SyntaxError
                        assert children[2].data == 'column_name'
                        colNameTree = children[2]
                        colName = colNameTree.children[0].value
                        assert children[3].type == 'REFERENCES' and children[4].data == 'table_name' and \
                               children[5].data == 'column_name_list'
                        assert children[5].children[0].type == 'LP' and children[5].children[2].type == 'RP'
                        referredTableName = children[4].children[0].value
                        referredColName = children[5].children[2].value
                        newTable.setForeignKey(colName, referredTableName, referredColName)
                else:
                    raise SyntaxError
            self.tables.append(newTable)
        except:
            self._putInstruction('Syntax error')

    def _dropTable(self, query):
        self._putInstruction("DROP_TABLE_QUERY")
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
            print(self.tables)
        except:
            self._putInstruction('Syntax error')

    def _desc(self, query):
        try:
            descQuery = query.children[0]
            children = descQuery.children
            assert descQuery.data == 'desc_query'
            assert children[0].type == 'DESC' and children[1].data == 'table_name'
            tableName = children[1].children[0].value
            # TODO: 추후 업데이트 해야 될 영역.
            return tableName
        except:
            self._putInstruction('Syntax error')

    def _insert(self, query):
        pass

    def _delete(self, query):
        pass

    def _select(self, query):
        pass

    def _showTables(self, query):
        pass


class Table:
    def __init__(self, name: str, cols=[]):
        self.name = name
        self.cols = cols
        self.pKeys = []
        self.fKeys = []

    def addCol(self, col: dict):
        self.cols.append(col)

    def _addKey(self, keyList: list, colName):
        if colName not in keyList:
            isExist = False
            for col in self.cols:
                if col['name'] == colName:
                    isExist = True
                    break
            if isExist:
                keyList.append(colName)
                return 1
            return 0
        return 1

    def setPrimaryKey(self, colName):
        return self._addKey(self.pKeys, colName)

    def setForeignKey(self, colName, referredTableName: str, referredColName: str):
        isExist = self._addKey(self.fKeys, colName)
        if isExist == 1:
            for col in self.cols:
                if col['name'] == colName:
                    col['referredColumn'] = referredTableName + '.' + referredColName
                    break
        return isExist


DB = DataBase()
DB.getUserInput(True)
