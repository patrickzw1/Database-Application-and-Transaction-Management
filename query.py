from flask import Flask, g, request, jsonify
import pyodbc
from connect_db import connect_db
import sys
import time, datetime


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'azure_db'):
        g.azure_db = connect_db()
        g.azure_db.autocommit = True
        g.azure_db.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
    return g.azure_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'azure_db'):
        g.azure_db.close()



@app.route('/login')
def login():
    username = request.args.get('username', "")
    password = request.args.get('password', "")
    cid = -1
    #print (username, password)
    conn = get_db()
    #print (conn)
    cursor = conn.execute("SELECT * FROM Customer WHERE username = ? AND password = ?", (username, password))
    records = cursor.fetchall()
    #print records
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)




@app.route('/getRenterID')
def getRenterID():
    """
       This HTTP method takes mid as input, and
       returns cid which represents the customer who is renting the movie.
       If this movie is not being rented by anyone, return cid = -1
    """
    mid = int(request.args.get('mid', -1))
    status = "open"

    # WRITE YOUR CODE HERE
    cid = -1
    conn = get_db()
    cursor = conn.execute("SELECT cid FROM Rental WHERE mid = ? AND status = ?", (mid, status))
    records = cursor.fetchall()

    if len(records) != 0:
        cid = records[0][0]

    response = {'cid': cid}
    return jsonify(response)



@app.route('/getRemainingRentals')
def getRemainingRentals():
    """
        This HTTP method takes cid as input, and returns n which represents
        how many more movies that cid can rent.

        n = 0 means the customer has reached its maximum number of rentals.
    """
    cid = int(request.args.get('cid', -1))

    pid = -1
    max_movies = -1
    movie_rented = 0

    conn = get_db()

    status = "open"

    # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    cursor = conn.execute("SELECT pid FROM Customer WHERE cid = ?", (cid))
    records = cursor.fetchall()
    if len(records) != 0:
        pid = records[0][0]
    cursor = conn.execute("SELECT max_movies FROM RentalPlan WHERE pid = ?", (pid))
    records = cursor.fetchall()
    if len(records) != 0:
        max_movies = records[0][0]
    cursor = conn.execute("SELECT COUNT(*) FROM Rental WHERE cid = ? AND status = ? GROUP BY cid", (cid,  status))
    records = cursor.fetchall()
    if len(records) != 0:
        movie_rented = records[0][0]

    conn.autocommit = True
    
    n = max_movies - movie_rented

    response = {"remain": n}
    return jsonify(response)





def currentTime():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@app.route('/rent')
def rent():
    """
        This HTTP method takes cid and mid as input, and returns either "success" or "fail".

        It returns "fail" if C1, C2, or both are violated:
            C1. at any time a movie can be rented to at most one customer.
            C2. at any time a customer can have at most as many movies rented as his/her plan allows.
        Otherwise, it returns "success" and also updates the database accordingly.
    """
    cid = int(request.args.get('cid', -1))
    mid = int(request.args.get('mid', -1))
    pid = -1
    max_movies = -1
    movie_rented = 0
    status = "open"
    latestDate = -1
    rent = "success"

    conn = get_db()

     # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    cursor = conn.execute("SELECT pid FROM Customer WHERE cid = ?", (cid))
    records = cursor.fetchall()
    if len(records) != 0:
        pid = records[0][0]

    cursor = conn.execute("SELECT max_movies FROM RentalPlan WHERE pid = ?", (pid))
    records = cursor.fetchall()
    if len(records) != 0:
        max_movies = records[0][0]

    cursor = conn.execute("SELECT COUNT(*) FROM Rental WHERE cid = ? AND status = ? GROUP BY cid", (cid,  status))
    records = cursor.fetchall()
    if len(records) != 0:
        movie_rented = records[0][0]

    n = max_movies - movie_rented

    if n <= 0:
        rent = "fail"

    cursor = conn.execute("SELECT cid FROM Rental WHERE mid = ? AND status = ?", (mid, status))
    records = cursor.fetchall()
    #print(records)
    if len(records) != 0:
        rent = "fail"

    date = currentTime()
    if rent == "success":
        cursor = conn.execute("SELECT cid FROM Rental WHERE cid = ? AND mid = ? AND status = ?", (cid, mid, "closed"))
        records = cursor.fetchall()
        if len(records) != 0:
            conn.execute("UPDATE Rental SET date_and_time = ?, status = ? WHERE cid = ? AND mid = ?", (date, status, cid, mid))
        else:
            update = (cid, mid, date, status)
            conn.execute("INSERT INTO Rental(cid, mid, date_and_time, status) VALUES (?,?,?,?)", update)
    # if len(records) != 0:
    #     if len(records) > 1:
    #         for row in records:
    #             if row+1 <= len(records):
    #                 if records[row][1] > records[row+1][1]:
    #                     status = records[row][0]
    #                     latestDate = records[row][1]
    #                 status = records[row+1][0]
    #                 latestDate = records[row+1][1]
    #     status = records[0][0]

    # print("status: "+status)
    # if status == "open":
    #     response = {"rent": "fail"}

    # if status == "":
    #     response = {"rent": "success"}
    #     date = currentTime()
    #     staus = "open"
    #     update = (cid, mid, date, status)
    #     conn.execute("INSERT INTO Rental(cid, mid, date_and_time, status) VALUES (?,?,?,?)", update)
    
    # if status == "closed":

    #         date = currentTime()
    #         status = "open"
    #         update = (cid, mid, date, status)
    #         conn.execute("UPDATE Rental SET cid = ?, mid = ?, date_and_time = ?, status = ?", update)



    conn.autocommit = True

    if rent == "fail":
        conn.rollback()
    response = {"rent": rent}
    #response = {"rent": "success"} OR response = {"rent": "fail"}
    return jsonify(response)

