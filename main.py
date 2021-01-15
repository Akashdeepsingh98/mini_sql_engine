import itertools
import json
from moz_sql_parser import parse

Operators = {
    "gte": (lambda x, y: x >= y),
    "lte": (lambda x, y: x <= y),
    "gt": (lambda x, y: x > y),
    "lt": (lambda x, y: x < y),
    "eq": (lambda x, y: x == y)
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


def Parse(tables):
    userin = "SELECT * FROM table1,table2 where A>900"
    tokens = parse(userin)
    print(tokens)

    if tokens['select'] == '*':
        data = None
        columns = None
        if isinstance(tokens['from'], list):
            data = list(itertools.product(
                *[tables[table_name]["data"] for table_name in tokens['from']]))
            cleandata = []
            for i in range(len(data)):
                row = list(itertools.chain.from_iterable(list(data[i])))
                cleandata.append(row)
            data = cleandata

            columns = []
            for table_name in tokens['from']:
                for column in tables[table_name]["columns"]:
                    columns.append(column)

            if 'where' in tokens.keys():
                if 'and' in tokens['where'].keys():
                    cond = []
                    i = 0
                    for condition in tokens['where']['and']:
                        cond.append([])
                        for key in condition.keys():
                            cond[i].append(tokens['where']['and'][i][key][0])
                            cond[i].append(key)
                            cond[i].append(tokens['where']['and'][i][key][1])
                            for table_name in tables.keys():
                                if tokens['where']['and'][i][key][0] in tables[table_name]["columns"]:
                                    cond[i].append(table_name)
                        i += 1
                    col1 = cond[0][0]
                    col2 = cond[1][0]
                    comp1 = cond[0][1]
                    comp2 = cond[1][1]
                    bound1 = cond[0][2]
                    bound2 = cond[1][2]
                    col1ind = columns.index(col1)
                    col2ind = columns.index(col2)
                    cleanerdata = []
                    for row in data:
                        if (Operators[comp1](row[col1ind], bound1)) and (Operators[comp2](row[col2ind], bound2)):
                            cleanerdata.append(row)
                    data = cleanerdata

                elif 'or' in tokens['where'].keys():
                    cond = []
                    i = 0
                    for condition in tokens['where']['and']:
                        cond.append([])
                        for key in condition.keys():
                            cond[i].append(tokens['where']['and'][i][key][0])
                            cond[i].append(key)
                            cond[i].append(tokens['where']['and'][i][key][1])
                            for table_name in tables.keys():
                                if tokens['where']['and'][i][key][0] in tables[table_name]["columns"]:
                                    cond[i].append(table_name)
                        i += 1
                    col1 = cond[0][0]
                    col2 = cond[1][0]
                    comp1 = cond[0][1]
                    comp2 = cond[1][1]
                    bound1 = cond[0][2]
                    bound2 = cond[1][2]
                    col1ind = columns.index(col1)
                    col2ind = columns.index(col2)
                    cleanerdata = []
                    for row in data:
                        if (Operators[comp1](row[col1ind], bound1)) or (Operators[comp2](row[col2ind], bound2)):
                            cleanerdata.append(row)
                    data = cleanerdata

                else:
                    col = None
                    bound = None
                    comp = None
                    for key in tokens['where'].keys():
                        comp = key
                        col = tokens['where'][key][0]
                        bound = tokens['where'][key][1]
                    colind = columns.index(col)
                    print(colind)
                    print(col)
                    print(bound)
                    print(comp)
                    cleanerdata = []
                    for i in range(len(data)):
                        if Operators[comp](data[i][colind], bound):
                            cleanerdata.append(data[i])
                        #    data.remove(row)
                    data = cleanerdata

        else:
            table_name = tokens['from']
            data = tables[table_name]["data"]
            columns = tables[table_name]["columns"]

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
