import pyodbc
from connect_db import connect_db


def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    cursor = conn.cursor()

    sql_command = """
    CREATE TABLE RentalPlan(
    pid INTEGER PRIMARY KEY,
    pname VARCHAR(50),
    monthly_fee FLOAT,
    max_movies INTEGER);
    """
    
    cursor.execute(sql_command)

    informationList = []
    with open("RentalPlan.txt") as file:
        for line in file:
            line = line.strip('\n')
            line = line.split("|")
            informationList.append(line)
            #print (informationList)

    sql_command = """ INSERT INTO RentalPlan(
                      pid, pname, monthly_fee, max_movies)
                      VALUES (?,?,?,?)"""
                      #in pyodbc use ? instead %
    cursor.executemany(sql_command, informationList)

    

def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    cursor = conn.cursor()

    sql_command = """
    CREATE TABLE Customer(
    cid INTEGER PRIMARY KEY,
    pid INTEGER,
    username VARCHAR(50),
    password VARCHAR(50),
    FOREIGN KEY (pid) REFERENCES RentalPlan (pid)
    ON DELETE CASCADE);
    """
    
    cursor.execute(sql_command)

    informationList = []
    with open("Customer.txt") as file:
        for line in file:
            line = line.strip('\n')
            line = line.split("|")
            informationList.append(line)
            #print(informationList)

    sql_command = """INSERT INTO Customer(
                     cid, pid, username, password) VALUES(
                     ?,?,?,?)"""
    cursor.executemany(sql_command, informationList)

def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    cursor = conn.cursor()

    sql_command = """
    CREATE TABLE Movie(
    mid INTEGER PRIMARY KEY,
    mname VARCHAR(50),
    year INTEGER);
    """
    
    cursor.execute(sql_command)

    informationList = []
    with open("Movie.txt") as file:
        for line in file:
            line = line.strip('\n')
            line = line.split("|")
            informationList.append(line)
            #print(informationList)

    sql_command = """INSERT INTO Movie(
                     mid, mname, year) VALUES(
                     ?,?,?)"""
    cursor.executemany(sql_command, informationList)

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    cursor = conn.cursor()

    sql_command = """
    CREATE TABLE Rental(
    cid INTEGER,
    mid INTEGER,
    date_and_time DATETIME,
    status VARCHAR(6),
    FOREIGN KEY (cid) REFERENCES Customer(cid) ON DELETE CASCADE,
    FOREIGN KEY (mid) REFERENCES Movie(mid) ON DELETE CASCADE);"""

    cursor.execute(sql_command)

    informationList = []
    with open("Rental.txt") as file:
        for line in file:
            line = line.strip('\n')
            line = line.split("|")
            informationList.append(line)
            #print(informationList)

    sql_command = """INSERT INTO Rental(
                     cid, mid, date_and_time, status) VALUES
                     (?,?,?,?)"""
    cursor.executemany(sql_command, informationList)


def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






