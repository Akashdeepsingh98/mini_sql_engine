import itertools
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
        #row = list(map(int, row.strip().split(',')))
        row = row.split(',')
        for i in range(len(row)):
            if isinstance(row[i], str):
                row[i] = int(row[i].strip().strip('"').strip("'"))
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
    distinctset = []
    # get indexes of columns
    indexes = []
    for column in columns:
        indexes.append(tables[table_name]["columns"].index(column))
    for i in range(len(tables[table_name]["data"])):
        row = []
        for j in indexes:
            row.append(tables[table_name]["data"][i][j])
        if row not in distinctset:
            distinctset.append(row)
    return distinctset


def Parse(tables, query):
    #userin = "SELECT distinct A,B from table1;"
    tokens = parse(query)
    aggregatework = False
    print(tokens)

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
                    if (Operators[comp1](row[col1ind], bound1 if isinstance(bound1, int) else row[columns.index(bound1)])) and (Operators[comp2](row[col2ind], bound2 if isinstance(bound2, int) else row[columns.index(bound2)])):
                        cleanerdata.append(row)
                data = cleanerdata
            elif 'or' in tokens['where'].keys():
                cond = []
                i = 0
                for condition in tokens['where']['or']:
                    cond.append([])
                    for key in condition.keys():
                        cond[i].append(tokens['where']['or'][i][key][0])
                        cond[i].append(key)
                        cond[i].append(tokens['where']['or'][i][key][1])
                        for table_name in tables.keys():
                            if tokens['where']['or'][i][key][0] in tables[table_name]["columns"]:
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
                    if (Operators[comp1](row[col1ind], bound1 if isinstance(bound1, int) else row[columns.index(bound1)])) or (Operators[comp2](row[col2ind], bound2 if isinstance(bound2, int) else row[columns.index(bound2)])):
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
                cleanerdata = []
                for i in range(len(data)):
                    if Operators[comp](data[i][colind], bound if isinstance(bound, int) else data[i][columns.index(bound)]):
                        cleanerdata.append(data[i])
                data = cleanerdata
    # just one table
    else:
        table_name = tokens['from']
        data = tables[table_name]["data"]
        columns = tables[table_name]["columns"]
        if not tokens['select'] == '*' and not isinstance(tokens['select'], list) and isinstance(tokens['select']['value'], dict):
            aggregatework = True
            if 'max' in tokens['select']['value'].keys():
                result = Max(tables, tokens['from'],
                             tokens['select']['value']['max'])
                print('max('+tokens['from']+'.' +
                      tokens['select']['value']['max'].lower()+')')
                print(result)
            elif 'min' in tokens['select']['value'].keys():
                result = Min(tables, tokens['from'],
                             tokens['select']['value']['min'])
                print('min('+tokens['from']+'.' +
                      tokens['select']['value']['min'].lower()+')')
                print(result)
            elif 'count' in tokens['select']['value'].keys():
                result = Count(tables, tokens['from'],
                               tokens['select']['value']['count'])
                print('count('+tokens['from']+'.' +
                      tokens['select']['value']['count'].lower()+')')
                print(result)
            elif 'sum' in tokens['select']['value'].keys():
                result = Sum(tables, tokens['from'],
                             tokens['select']['value']['sum'])
                print('sum('+tokens['from']+'.' +
                      tokens['select']['value']['sum'].lower()+')')
                print(result)
            elif 'avg' in tokens['select']['value'].keys():
                result = Average(tables, tokens['from'],
                                 tokens['select']['value']['avg'])
                print('avg('+tokens['from']+'.' +
                      tokens['select']['value']['avg'].lower()+')')
                print(result)
            elif 'distinct' in tokens['select']['value'].keys():
                result = None
                cleanercols = []
                if isinstance(tokens['select']['value']['distinct'], list):
                    for value in tokens['select']['value']['distinct']:
                        cleanercols.append(value['value'])
                    pass
                else:
                    cleanercols.append(tokens['select']['value']['distinct'])
                    pass
                data = Distinct(tables, table_name, cleanercols)
                columns = cleanercols
                Project(data, columns)
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
                    if (Operators[comp1](row[col1ind], bound1 if isinstance(bound1, int) else row[columns.index(bound1)])) and (Operators[comp2](row[col2ind], bound2 if isinstance(bound2, int) else row[columns.index(bound2)])):
                        cleanerdata.append(row)
                data = cleanerdata
            elif 'or' in tokens['where'].keys():
                cond = []
                i = 0
                for condition in tokens['where']['or']:
                    cond.append([])
                    for key in condition.keys():
                        cond[i].append(tokens['where']['or'][i][key][0])
                        cond[i].append(key)
                        cond[i].append(tokens['where']['or'][i][key][1])
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
                    if (Operators[comp1](row[col1ind], bound1 if isinstance(bound1, int) else row[columns.index(bound1)])) or (Operators[comp2](row[col2ind], bound2 if isinstance(bound2, int) else row[columns.index(bound2)])):
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
                cleanerdata = []
                for i in range(len(data)):
                    if Operators[comp](data[i][colind], bound if isinstance(bound, int) else data[i][columns.index(bound)]):
                        cleanerdata.append(data[i])
                data = cleanerdata
    # order by work
    if 'orderby' in tokens.keys():
        col_ind = columns.index(tokens["orderby"]['value'])
        if 'sort' in tokens['orderby'].keys():
            if tokens['order']['sort'] == 'asc':
                data.sort(key=lambda x: x[col_ind])
            else:
                data.sort(key=lambda x: x[col_ind], reverse=True)
        else:
            data.sort(key=lambda x: x[col_ind])

    # group by work
    if 'groupby' in tokens.keys():
        groupcol = tokens['groupby']['value']
        groupcolind = columns.index(groupcol)
        table_name = None
        for tn in tables.keys():
            if groupcol in tables[tn]["columns"]:
                table_name = tn
                break

        distinctset = []
        cleanerdata = {}
        cleanercols = []
        for row in data:
            distinctset.append(row[groupcolind])
            if distinctset[-1] in distinctset[:-1]:
                distinctset.pop(-1)
            else:
                cleanerdata[distinctset[-1]] = [None]*len(tokens['select'])
                print(distinctset[-1])

        i = 0
        for col in tokens['select']:
            if isinstance(col['value'], dict):
                if 'max' in col['value'].keys():
                    cleanercols.append('max('+col['value']['max']+')')
                    colind = columns.index(col['value']['max'])
                    for row in data:
                        distinctval = row[groupcolind]
                        cleanerdata[distinctval][i] = cleanerdata[distinctval][
                            i] if (cleanerdata[distinctval][i] != None and cleanerdata[distinctval][i] > row[colind]) else row[colind]
                elif 'count' in col['value'].keys():
                    cleanercols.append('count('+col['value']['count']+')')
                    colind = columns.index(col['value']['count'])
                    for row in data:
                        distinctval = row[groupcolind]
                        cleanerdata[distinctval][i] = 1 if cleanerdata[distinctval][i] == None else cleanerdata[distinctval][i]+1
                elif 'min' in col['value'].keys():
                    cleanercols.append('min('+col['value']['min']+')')
                    colind = columns.index(col['value']['min'])
                    for row in data:
                        distinctval = row[groupcolind]
                        cleanerdata[distinctval][i] = cleanerdata[distinctval][
                            i] if (cleanerdata[distinctval][i] != None and cleanerdata[distinctval][i] < row[colind]) else row[colind]
                elif 'sum' in col['value'].keys():
                    cleanercols.append('sum('+col['value']['sum']+')')
                    colind = columns.index(col['value']['sum'])
                    for row in data:
                        distinctval = row[groupcolind]
                        cleanerdata[distinctval][i] = cleanerdata[distinctval][i] + \
                            row[colind] if cleanerdata[distinctval][i] != None else row[colind]
                elif 'avg' in col['value'].keys():
                    cleanercols.append('avg('+col['value']['avg']+')')
                    colind = columns.index(col['value']['avg'])
                    counter = {}
                    for distinctval in distinctset:
                        counter[distinctval] = 0
                    for row in data:
                        distinctval = row[groupcolind]
                        counter[distinctval] += 1
                        cleanerdata[distinctval][i] = cleanerdata[distinctval][i] + \
                            row[colind] if cleanerdata[distinctval][i] != None else row[colind]
                    for distinctval in cleanerdata.keys():
                        cleanerdata[distinctval][i] /= counter[distinctval]
            else:
                colind = columns.index(col['value'])
                cleanercols.append(col['value'])
                for row in data:
                    distinctval = row[groupcolind]
                    cleanerdata[distinctval][i] = row[colind]
            i += 1
        columns = cleanercols
        data = []
        for row in cleanerdata.values():
            data.append(row)
    # if no group by then select columns
    elif tokens['select'] != '*' and not aggregatework:
        cleanerdata = []
        cleanercols = []
        colinds = []
        for col in tokens['select']:
            colinds.append(columns.index(col['value']))
            cleanercols.append(col['value'])
        for row in data:
            cleanerdata.append([])
            for ind in colinds:
                cleanerdata[-1].append(row[ind])
            if cleanerdata[-1] in cleanerdata[:-1]:
                cleanerdata.pop(-1)
        columns = cleanercols
        data = cleanerdata
    if not aggregatework:
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
    query = input()
    Parse(tables, query)
