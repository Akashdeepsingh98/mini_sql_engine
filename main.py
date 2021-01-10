def print_tables(tables):
    for table in tables:
        print(table["table_name"])
        print(table["columns"])
        for column in table["columns"]:
            print(column+': '+str(table["data"][column]))


def get_table(Lines, start):
    # start reading from this tag
    start += 1
    table_name = Lines[start].strip()   # get table name
    start += 1
    table = {"table_name": table_name,
             "columns": [],
             "data": {}}

    while Lines[start].strip() != '<end_table>':  # read all column names of this table
        table["columns"].append(Lines[start].strip())
        table["data"][Lines[start].strip()] = []
        start += 1
    start += 1

    # start reading data file line by line
    datafile = open(table_name+'.csv', 'r')
    print(table["columns"])
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
    return table, start


def get_tables():
    tables = []
    metadata = open('metadata.txt', 'r')
    Lines = metadata.readlines()
    i = 0
    while i < len(Lines)-1:
        table, i = get_table(Lines, i)
        tables.append(table)
    return tables


if __name__ == '__main__':
    tables = get_tables()
    print_tables(tables)
