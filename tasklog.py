import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import mysql.connector
import sys

import datetime


def get_conf(filename, parameters, lists):
    conf_dict = {}

    for line in open(filename):
        if line.split("=")[0].strip() in parameters:
            conf_dict[line.split("=")[0].strip()] = line.split("=")[1].strip()
        elif line.split("=")[0].strip() in lists:
            parameter_list = []

            for item in line.split("=")[1].strip().split(","):
                parameter_list.append(item.strip())

            conf_dict[line.split("=")[0].strip()] = parameter_list

    return conf_dict


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
    id_in_table = True

    verbose_logging = False
    conf_path = "../conf/tasklog.conf"
    conf_parameters = ["db_username", "db_password", "db_host", "database", "db_table", "firebase_admin_certificate"]
    conf_lists = ["rows"]

    if verbose_logging:
        print("Start")

    for arg in sys.argv:
        if arg == "-v":
            verbose_logging = True
        elif arg == "-conf":
            try:
                conf_file = sys.argv[sys.argv.index(arg) + 1]
            except IndexError:
                if verbose_logging:
                    print("File di configurazione non valido")
                exit(1)

    conf = get_conf(conf_path, conf_parameters, conf_lists)

    processes = {}

    try:
        connection = mysql.connector.connect(user=conf["db_username"], password=conf["db_password"],
                                             host=conf["db_host"], database=conf["database"])
    except mysql.connector.errors.InterfaceError:
        if verbose_logging:
            print("Impossibile connettersi al database")

        exit(1)

    database_cursor = connection.cursor(buffered=True)

    try:
        result_set = database_query(database_cursor, "select * from " + conf["db_table"] + " order by timestamp desc")
    except mysql.connector.errors.ProgrammingError:
        if verbose_logging:
            print("Errore di sintassi nella query al database")

        exit(1)

    for result in result_set:
        process = {}
        for i in range(2, len(conf["rows"]) + 1):
            if type(result[i]) is datetime.datetime:
                process[conf["rows"][i - 1]] = str(result[i])
            else:
                process[conf["rows"][i - 1]] = result[i]

        if result[1] not in processes.keys():
            processes[result[1]] = process

    # firebase

    try:
        firebase_connection(conf["firebase_admin_certificate"])
    except IOError:
        exit(1)
    except Exception:
        exit(1)

    db_ref = "/"
    structure = processes

    firebase_update(db_ref, structure)

    if verbose_logging:
        print("Stop")
