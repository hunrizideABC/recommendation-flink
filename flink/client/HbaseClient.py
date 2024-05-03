import happybase
class HBaseClient:
    def __init__(self):
        self.connection = happybase.Connection(host='43.130.37.56', port=9090)

    def createTable(self, table_name, column_families):
        self.connection.open()
        if table_name.encode() in self.connection.tables():
            print("Table Exists")
            return
        else:
            print("Start Create Table")
            self.connection.create_table(
                table_name,
                {column_family: dict(max_versions=2) for column_family in column_families}
            )
            print("Create Table Success")
        self.connection.close()

    def deleteTable(self, table_name):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table Do Not Exists")
            return
        else:
            print("Start Delete Table")
            self.connection.delete_table(table_name, disable=True)
            print("Delete Table Success")
        self.connection.close()

    def insertData(self, table_name, row_key, family_name, column, data):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table Do Not Exists")
            return
        else:
            table = self.connection.table(table_name)
            table.put(row_key, {f"{family_name}:{column}": data})
        self.connection.close()

    def getData(self, table_name, row_key, family_name, column):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table Do Not Exists")
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
            print("Table Do Not Exists")
            return
        else:
            print("Start Delete Data")
            table = self.connection.table(table_name)
            row = table.delete(row_key.encode(), [f"{family_name}:{column}".encode()])
            print("Delete Data Success")
        return

    def getRow(self, table_name, row_key):
        self.connection.open()
        if table_name.encode() not in self.connection.tables():
            print("Table Do Not Exists")
            return
        else:
            table = self.connection.table(table_name)
            row = table.row(row_key.encode())
            return row


if __name__ == "__main__":
    hbaseClient = HBaseClient()
    hbaseClient.createTable('user', ['country', 'color', 'style'])
    hbaseClient.insertData('user', '1', 'color', 'blue', '3')
    hbaseClient.insertData('user', '1', 'color', 'red', '10')
    data = hbaseClient.getData('user', '1', 'color', 'blue')
    print(data)
    hbaseClient.deleteData('user', '1', 'color', 'blue')