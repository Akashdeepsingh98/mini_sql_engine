def Print_tables(tables):
    for table in tables.keys():
        print(table)
        print(tables[table]["columns"])
        for column in tables[table]["columns"]:
            print(column+': '+str(tables[table]["data"][column]))


def Get_table(Lines, start):
    # start reading from this tag
    start += 1
    table_name = Lines[start].strip()   # get table name
    start += 1
    table = {"columns": [],
             "data": {}}

    while Lines[start].strip() != '<end_table>':  # read all column names of this table
        table["columns"].append(Lines[start].strip())
        table["data"][Lines[start].strip()] = []
        start += 1
    start += 1

    # start reading data file line by line
    datafile = open(table_name+'.csv', 'r')
    # print(table["columns"])
    while True:
        row = datafile.readline()
        if not row:
            break
        # convert row to list of ints
        row = list(map(int, row.strip().split(',')))
        i = 0
        # go through each element in a row and store in proper column
        for column in table["columns"]:
            # print(row[i])
            table["data"][column].append(row[i])
            i += 1
    return table_name, table, start


def Project(tables, table_name, columns):
    for column in columns:
        print(column, end='   ')
    print()
    # need to print the column names and data in a column aligned fashion
    for i in range(len(tables[table_name]["data"][columns[0]])):
        for column in columns:
            print(str(tables[table_name]["data"][column][i]), end='   ')
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
    print("sum("+column+")")
    print(sum(tables[table_name]["data"][column]))


def Average(tables, table_name, column):
    print("average("+column+")")
    print(sum(tables[table_name]["data"][column]) /
          len(tables[table_name]["data"][column]))


def Max(tables, table_name, column):
    print("max("+column+")")
    print(max(tables[table_name]["data"][column]))


def Min(tables, table_name, column):
    print(column)
    print(min(tables[table_name]["data"][column]))


def Count(tables, table_name, column):
    print("count("+column+")")
    print(len(tables[table_name]["data"][column]))


def Distinct(tables, table_name, columns):
    distinctset = set()
    for i in range(len(tables[table_name]["data"][columns[0]])):
        row = []
        for column in columns:
            row.append(tables[table_name]["data"][column][i])
        distinctset.add(tuple(row))
    for column in columns:
        print(column, end='   ')
    print()
    for s in distinctset:
        for i in range(len(columns)):
            print(s[i], end="   ")
        print()


if __name__ == '__main__':
    tables = Get_tables()
    # print_tables(tables)
    Project(tables, 'table1', ['A', 'B'])
    #Sum(tables, 'table1', 'A')
    #Average(tables, 'table1', 'A')
    #Max(tables, 'table1', 'A')
    #Min(tables, 'table1', 'A')
    #Count(tables, 'table1', 'A')
    Distinct(tables, 'table1', ['A', 'B'])
