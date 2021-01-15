import itertools
import json
from moz_sql_parser import parse

Operators = {
    ">=": (lambda x, y: x >= y),
    "<=": (lambda x, y: x <= y),
    ">": (lambda x, y: x > y),
    "<": (lambda x, y: x < y),
    "=": (lambda x, y: x == y)
}

Binary = {
    "AND": (lambda x, y, z, w: x == y and z == w),
    "OR": (lambda x, y, z, w: x == y or z == w)
}


def Print_tables(tables):
    for table in tables.keys():
        print(table)
        print(tables[table]["columns"])
        for row in tables[table]['data']:
            print(row)


def Get_table(Lines, start):
    # start reading from this tag
    start += 1
    table_name = Lines[start].strip()   # get table name
    start += 1
    table = {"columns": [],
             "data": []}

    while Lines[start].strip() != '<end_table>':  # read all column names of this table
        table["columns"].append(Lines[start].strip())
        start += 1
    start += 1

    # start reading data file line by line
    datafile = open(table_name+'.csv', 'r')

    while True:
        row = datafile.readline()
        if not row:
            break
        row = list(map(int, row.strip().split(',')))
        table['data'].append(row)

    return table_name, table, start


def Project(data, columns):
    for column in columns:
        print(column, end='   ')
    print()
    for row in data:
        if not isinstance(row[0], int):
            unrolled = list(itertools.chain.from_iterable(row))
            for ele in unrolled:
                print(ele, end='   ')
            print()
        else:
            for ele in row:
                print(ele, end='   ')
            print()


def Get_tables():
    tables = {}
    metadata = open('metadata.txt', 'r')
    Lines = metadata.readlines()
    i = 0
    while i < len(Lines)-1:
        table_name, table, i = Get_table(Lines, i)
        tables[table_name] = table
    return tables


def Sum(tables, table_name, column):
    index = tables[table_name]["columns"].index(column)
    result = 0
    for i in range(len(tables[table_name]["data"])):
        result += tables[table_name]["data"][i][index]
    return result


def Average(tables, table_name, column):
    return Sum(tables, table_name, column) / len(tables[table_name]["data"])


def Max(tables, table_name, column):
    index = tables[table_name]["columns"].index(column)
    result = tables[table_name]["data"][0][index]
    for i in range(1, len(tables[table_name]["data"])):
        if tables[table_name]["data"][i][index] > result:
            result = tables[table_name]["data"][i][index]
    return result


def Min(tables, table_name, column):
    index = tables[table_name]["columns"].index(column)
    result = tables[table_name]["data"][0][index]
    for i in range(1, len(tables[table_name]["data"])):
        if tables[table_name]["data"][i][index] < result:
            result = tables[table_name]["data"][i][index]
    return result


def Count(tables, table_name, column):
    return(len(tables[table_name]["data"]))


def Distinct(tables, table_name, columns):
    distinctset = set()
    # get indexes of columns
    indexes = []
    for column in columns:
        indexes.append(tables[table_name]["columns"].index(column))
    for i in range(len(tables[table_name]["data"])):
        row = []
        for j in indexes:
            row.append(tables[table_name]["data"][i][j])
        distinctset.add(tuple(row))
    return distinctset


def Where(tables, table_name, columns, conditions):
    # conditions = {"type": "and", "col1": 23, "col2": 45}

    pass


def Parse(tables):
    userin = "SELECT * FROM table1,table2 order by A"
    tokens = parse(userin)
    print(tokens)
    # if 'orderby' in tokens.keys():
#
    #    pass
    if tokens['select'] == '*':
        data = None
        columns = None
        if isinstance(tokens['from'], list):
            data = list(itertools.product(
                *[tables[table_name]["data"] for table_name in tokens['from']]))
            columns = []
            for table_name in tokens['from']:
                for column in tables[table_name]["columns"]:
                    columns.append(column)
            #Project(data, columns)
        else:
            table_name = tokens['from']
            data = tables[table_name]["data"]
            columns = tables[table_name]["columns"]
            #Project(data, tables[table_name]["columns"])

        if 'orderby' in tokens.keys():
            col_ind = columns.index(tokens["orderby"]['value'])
            if 'sort' in tokens['orderby'].keys():
                if tokens['order']['sort'] == 'asc':
                    data.sort(key=lambda x: x[col_ind])
                else:
                    data.sort(key=lambda x: x[col_ind], reverse=True)
            else:
                data.sort(key=lambda x: x[col_ind])
        Project(data, columns)


if __name__ == '__main__':
    tables = Get_tables()
    # Print_tables(tables)
    #Project(tables, 'table1', ['A', 'B'])
    #print(Sum(tables, 'table1', 'A'))
    #print(Average(tables, 'table1', 'A'))
    #print(Max(tables, 'table1', 'A'))
    #print(Min(tables, 'table1', 'A'))
    #print(Count(tables, 'table1', 'A'))
    #Project(Distinct(tables, 'table1', ['A', 'B']), ['A', 'B'])
    Parse(tables)
