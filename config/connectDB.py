import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="",
    database="dw_boxofficevn"
)

if conn.is_connected():
    print("Kết nối MySQL thành công!")
else:
    print("Kết nối thất bại!")