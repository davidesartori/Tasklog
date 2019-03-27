#!/usr/bin/python
# -*- coding: utf-8 -*-

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import mysql.connector
import sys

import datetime
import time

__author__ = "Sartori Davide"
__version__ = "1.0"


def get_conf(filename, parameters, lists):
    """ returns the configuration from the given file """
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
    """ executes the given query on the database """
    cursor.execute(query)
    records = cursor.fetchall()

    return records


def firebase_connection(certificate):
    """ creates a connection with firebase """
    cred = credentials.Certificate(certificate)

    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://processi-marconi.firebaseio.com/'
    })


def firebase_update(ref_name, db_structure):
    """ updates the firebase database with the given structure """
    ref = db.reference(ref_name)

    ref.update(
        db_structure
    )


def format_log_text(text):
    """ returns the formatted log text """
    timestamp = datetime.date.today().strftime('%Y-%m-%d') + " " + time.strftime('%H:%M:%S')

    return timestamp + "§" + text + "\n"


if __name__ == "__main__":
    id_in_table = True

    verbose_logging = False
    conf_path = "tasklog.conf"
    log_path = "../log/tracelog.log"
    conf_parameters = ["db_username", "db_password", "db_host", "database", "db_table", "firebase_admin_certificate",
                       "log"]
    conf_lists = ["rows"]
    log_text = ""

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

    if conf["log"]:
        try:
            log_file = open(conf["log"], "a")
        except IOError:
            log_file = open(conf["log"], "w")

    processes = {}

    if verbose_logging:
        print("Connessione al database")

    log_text += format_log_text("Connessione al database")

    try:
        connection = mysql.connector.connect(user=conf["db_username"], password=conf["db_password"],
                                             host=conf["db_host"], database=conf["database"])
    except mysql.connector.errors.InterfaceError:
        if verbose_logging:
            print("Impossibile connettersi al database")

        log_text += format_log_text("Impossibile connettersi al database")

        log_file.write(log_text)

        exit(1)

    database_cursor = connection.cursor(buffered=True)

    if verbose_logging:
        print("Esecuzione query")

    log_text += format_log_text("Esecuzione query")

    try:
        result_set = database_query(database_cursor, "select * from " + conf["db_table"] + " order by timestamp desc")
    except mysql.connector.errors.ProgrammingError:
        if verbose_logging:
            print("Errore di sintassi nella query al database")

        log_text += format_log_text("Errore di sintassi nella query")

        log_file.write(log_text)

        exit(1)

    if verbose_logging:
        print("Chiusura connessione al database")

    log_text += format_log_text("Chiusura connessione al database")

    database_cursor.close()

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

    if verbose_logging:
        print("Connessione a firebase")

    log_text += format_log_text("Connessione a firebase")

    try:
        firebase_connection(conf["firebase_admin_certificate"])
    except IOError:
        if verbose_logging:
            print("Certificato admin non trovato")

        log_text += format_log_text("Certificato admin non trovato")

        log_file.write(log_text)

        exit(1)
    except Exception:
        if verbose_logging:
            print("Certificato admin non valido")

        log_text += format_log_text("Certificato admin non valido")

        log_file.write(log_text)

        exit(1)

    db_ref = "/"
    structure = processes

    if verbose_logging:
        print("Aggiornamento firebase")
    log_text += format_log_text("Aggiornamento firebase")

    firebase_update(db_ref, structure)

    if verbose_logging:
        print("Stop")

    log_text += format_log_text("0")

    log_file.write(log_text)
