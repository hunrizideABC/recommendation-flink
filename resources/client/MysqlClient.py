import mysql.connector

class MysqlClient:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(host="43.130.37.56", port="3306", user="root", passwd="123456", database="con", charset = "utf8", auth_plugin="mysql_native_password")
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as e:
            print("Error connecting to MySQL:", e)

    def selectByProductId(self, product_id):
        try:
            sql = "select * from product where product_id = {}".format(product_id)
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except mysql.connector.Error as e:
            print("Error executing MySQL query:", e)

    def selectByUserId(self, user_id):
        try:
            sql = "select  * from user where user_id = {}".format(user_id)
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except mysql.connector.Error as e:
            print("Error executing MySQL query:", e)

    def closeConnection(self):
        try:
            self.cursor.close()
            self.conn.close()
            print("MySQL connection closed.")
        except mysql.connector.Error as e:
            print("Error closing MySQL connection:", e)

if __name__ == "__main__":
    mysql_client = MysqlClient()
    result_set_product = mysql_client.selectByProductId(1)
    print(result_set_product)
    result_set_user = mysql_client.selectByUserId(1)
    print(result_set_user)
    mysql_client.closeConnection()