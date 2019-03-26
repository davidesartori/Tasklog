import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import mysql.connector

import datetime


def database_query(cursor, query):
    cursor.execute(query)
    records = cursor.fetchall()

    return records


def firebase_connection(certificate):
    cred = credentials.Certificate(certificate)

    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://processi-marconi.firebaseio.com/'
    })


def firebase_update(ref_name, db_structure):
    ref = db.reference(ref_name)

    ref.update(
        db_structure
    )


if __name__ == "__main__":
    db_username = "testuser"
    db_password = "password"
    db_host = "localhost"
    database = "processi_marconi"
    db_table = "Tasklog"
    rows = ["process", "timestamp", "event", "level"]
    id_in_table = True

    firebase_admin_certificate = "admin-sdk.json"

    verbose_logging = False

    processes = {}

    try:
        connection = mysql.connector.connect(user=db_username, password=db_password, host=db_host,
                                             database=database)
    except mysql.connector.errors.InterfaceError:
        exit(1)

    database_cursor = connection.cursor(buffered=True)

    result_set = database_query(database_cursor, "select * from " + db_table + " order by timestamp desc")

    for result in result_set:
        process = {}
        for i in range(2, len(rows) + 1):
            if type(result[i]) is datetime.datetime:
                process[rows[i - 1]] = str(result[i])
            else:
                process[rows[i - 1]] = result[i]

        if result[1] not in processes.keys():
            processes[result[1]] = process

    # firebase

    try:
        firebase_connection(firebase_admin_certificate)
    except IOError:
        exit(1)
    except Exception:
        exit(1)

    db_ref = "/"
    structure = processes

    firebase_update(db_ref, structure)
