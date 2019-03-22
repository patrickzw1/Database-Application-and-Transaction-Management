import pyodbc

def connect_db():
    ODBC_STR = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:cmpt354-zhenwu.database.windows.net,1433;Database=VideoStore;Uid=patrick@cmpt354-zhenwu;Pwd={w13465672638Z};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print (connect_db())
