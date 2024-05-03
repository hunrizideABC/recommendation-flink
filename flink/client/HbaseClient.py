import happybase
class HBaseClient:
    def __init__(self):
        self.connection = happybase.Connection(host='43.130.37.56', port=9090)

    def createTable(self, table_name, column_families):
        self.connection.open()
        if table_name.encode() in self.connection.tables():
            print("Table {} Exists".format(table_name))
            return
        else:
            print("Start Create Table: {}".format(table_name))
            self.connection.create_table(
                table_name,
                {column_family: dict(max_versions=2) for column_family in column_families}
            )
            print("Create Table Success:  {}".format(table_name))
        self.connection.close()

    def deleteTable(self, table_name):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table {} Do Not Exists".format(table_name))
            return
        else:
            print("Start Delete Table: {}".format(table_name))
            self.connection.delete_table(table_name, disable=True)
            print("Delete Table Success: {}".format(table_name))
        self.connection.close()

    def insertData(self, table_name, row_key, family_name, column, data):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table {} Do Not Exists".format(table_name))
            return
        else:
            table = self.connection.table(table_name)
            table.put(row_key, {f"{family_name}:{column}": data})
        self.connection.close()

    def getData(self, table_name, row_key, family_name, column):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table {} Do Not Exists".format(table_name))
            return
        else:
            table = self.connection.table(table_name)
            row = table.row(row_key.encode())
            if row:
                return row.get(f"{family_name}:{column}".encode(), None)
        return

    def deleteData(self, table_name, row_key, family_name, column):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table {} Do Not Exists".format(table_name))
            return
        else:
            print("Start Delete Table: {}; Data: {}".format(table_name, f"{family_name}:{column}"))
            table = self.connection.table(table_name)
            row = table.delete(row_key.encode(), [f"{family_name}:{column}".encode()])
            print("Delete Table: {}; Data: {} Success".format(table_name, f"{family_name}:{column}"))
        return

    def getRow(self, table_name, row_key):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table {} Do Not Exists".format(table_name))
            return
        else:
            table = self.connection.table(table_name)
            row = table.row(row_key.encode())
            return row


if __name__ == "__main__":
    hbaseClient = HBaseClient()
    hbaseClient.deleteTable('con')
    hbaseClient.deleteTable('prod')
    hbaseClient.deleteTable('u_history')
    hbaseClient.deleteTable('p_history')
    hbaseClient.deleteTable('u_interest')
    hbaseClient.deleteTable('ps')
    hbaseClient.deleteTable('px')
    hbaseClient.deleteTable('user')

    hbaseClient.createTable('con', ['log'])
    hbaseClient.createTable('prod', ['sex', 'age'])
    hbaseClient.createTable('u_history', ['p'])
    hbaseClient.createTable('p_history', ['p'])
    hbaseClient.createTable('u_interest', ['p'])
    hbaseClient.createTable('ps', ['p'])
    hbaseClient.createTable('px', ['p'])
    hbaseClient.createTable('user', ['country', 'color', 'style'])

    hbaseClient.insertData('user', '1', 'color', 'blue', '3')
    hbaseClient.insertData('user', '1', 'color', 'red', '10')
    data = hbaseClient.getData('user', '1', 'color', 'blue')
    print(data)
    row = hbaseClient.getRow('user', '1')
    print(row)
    hbaseClient.deleteData('user', '1', 'color', 'blue')
    data = hbaseClient.getData('user', '1', 'color', 'blue')
    print(data)