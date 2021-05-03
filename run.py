import lark


class Transformer(lark.Transformer):
    def command(self, items):
        if not isinstance(items[0], list):
            exit()
        return items[0]

    def query_list(self, items):
        return items

    def query(self, items):
        return dict(
            create_table_query="CREATE TABLE",
            drop_table_query="DROP TABLE",
            desc_query="DESC",
            show_tables_query="SHOW TABLES",
            select_query="SELECT",
            insert_query="INSERT",
            delete_query="DELTE"
        )[items[0].data]


def input_queries(prompt):
    s = input(prompt)
    if not s.strip():
        return []
    while not s.rstrip().endswith(';'):
        s += '\n' + input()
    return [x + ';' for x in s.split(';')[:-1]]


if __name__ == "__main__":
    prompt = "DB_example> "

    with open('../../../Downloads/project 1-1 sample code/grammar.lark') as file:
        parser = lark.Lark(file.read(), start="command", lexer='standard')
    transformer = Transformer()

    while True:
        for query in input_queries(prompt):
            try:
                tree = parser.parse(query)
                msg = transformer.transform(tree)[0]
                print(prompt + f"'{msg}' requests")
            except lark.exceptions.UnexpectedInput:
                print(prompt + "SYNTAX ERROR")
