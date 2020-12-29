import json
import time
from datetime import datetime

import psycopg2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


def citirePraguri(fisierPraguri):
    """
    functia care adauga pragurile din fisierul text intr-un dictionar
    """
    with open(fisierPraguri) as json_file:
        praguri_data = json.load(json_file)


# print(praguri_data)
# print(praguri_data["Marketing"])

class Watcher:
    DIRECTORY_TO_WATCH = "D:\ExpenseAlert\Facturi"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            print("Received created event - %s." % event.src_path)


# stabilirea conexiunii cu baza de date
conn = psycopg2.connect("dbname=expenses user=postgres password=root")
cursor = conn.cursor()


def adaugareFactura(factura):
    """
    functia care adauga o factura noua in baza de date
    """
    try:
        data_factura = datetime.strptime(factura["Data"], '%d/%m/%Y')
        data_factura = data_factura.date()
        print(data_factura)

        postgres_insert_query = """ INSERT INTO facturi (furnizor, tip, data, categorie, valoare) VALUES (%s,%s,%s,%s,%s)"""
        record_to_insert = (
            factura["Furnizor"], factura["Tip"], data_factura, factura["Categorie"], factura["Valoare"])
        cursor.execute(postgres_insert_query, record_to_insert)

        conn.commit()
        count = cursor.rowcount
        print(count, "Inregistrare adaugat cu succes in tabela furnizor")

    except (Exception, psycopg2.Error) as error:
        if (conn):
            print("Inregistrarea nu a fost adaugat in tabela furnizor", error)


def verifDepasire():
    """
    functia care verifica daca s-a depasit limita pentru o anumita categorie, in cazul in care ca s-a depasit
    va fi afisata o alerta
    """


citirePraguri("PraguriCategorii.txt")
exfactura = {"Furnizor": "DelGaz", "Tip": "Factura Gaz", "Data": "27/09/2020", "Categorie": "Utilitati", "Valoare": 350}
adaugareFactura(exfactura)

# inchidem conexiunea cu baza de date.
if (conn):
    cursor.close()
    conn.close()
    print("PostgreSQL connection is closed")
