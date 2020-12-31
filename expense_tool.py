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
    with open(fisierPraguri, "r", encoding='utf-8') as json_file:
        praguri_data = json.load(json_file)
    return praguri_data


def citireFactura(fisierFactura):
    """
    functia care adauga pragurile din fisierul text intr-un dictionar
    """
    with open(fisierFactura, "r", encoding='utf-8') as json_file:
        factura_data = json.load(json_file)
    return factura_data


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
            print("Am primit o factura noua - %s." % event.src_path)
            time.sleep(3)
            factura = citireFactura(event.src_path)
            adaugareFactura(factura)
            verifDepasire(dictionar_praguri)


# stabilirea conexiunii cu baza de date
conn = psycopg2.connect("dbname=facturi user=postgres password=root")
cursor = conn.cursor()


def adaugareFactura(factura_noua):
    """
    functia care adauga o factura noua in baza de date
    """
    try:
        data_factura = datetime.strptime(factura_noua["Data"], '%d/%m/%Y')
        data_factura = data_factura.date()
        print(data_factura)

        postgres_insert_query = """ INSERT INTO factura (furnizor, tip, Data, categorie, valoare) VALUES (%s,%s,%s,%s,%s)"""
        if factura_noua["Categorie"] == "":
            factura_noua["Categorie"] = "Diverse"
        record_to_insert = (
            factura_noua["Furnizor"], factura_noua["Tip"], data_factura, factura_noua["Categorie"],
            factura_noua["Valoare"])
        cursor.execute(postgres_insert_query, record_to_insert)

        conn.commit()
        count = cursor.rowcount
        print(count, "Inregistrare adaugata cu succes in tabela factura")

    except (Exception, psycopg2.Error) as error:
        if (conn):
            print("Inregistrarea nu a fost adaugata in tabela factura", error)


def verifDepasire(dictionar_praguri):
    """
    functia care verifica daca s-a depasit limita pentru o anumita categorie, in cazul in care ca s-a depasit
    va fi afisata o alerta
    """
    cursor.execute(""" SELECT valoare FROM factura where categorie='Marketing' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Marketing' este",suma)
    if suma > dictionar_praguri['Marketing']:
        print("Atentie, bugetul alocat pentru categoria 'Marketing' a fost depasit cu",
              suma - dictionar_praguri['Marketing'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Salarii' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Salarii' este",suma)
    if suma > dictionar_praguri['Salarii']:
        print("Atentie, bugetul alocat pentru categoria 'Salarii' a fost depasit cu",
              suma - dictionar_praguri['Salarii'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Tehnologie' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Tehnologie' este",suma)
    if suma > dictionar_praguri['Tehnologie']:
        print("Atentie, bugetul alocat pentru categoria 'Tehnologie' a fost depasit cu",
              suma - dictionar_praguri['Tehnologie'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Echipamente' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Echipamente' este",suma)
    if suma > dictionar_praguri['Echipamente']:
        print("Atentie, bugetul alocat pentru categoria 'Echipamente' a fost depasit cu",
              suma - dictionar_praguri['Echipamente'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Transport' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Transport' este",suma)
    if suma > dictionar_praguri['Transport']:
        print("Atentie, bugetul alocat pentru categoria 'Transport' a fost depasit cu",
              suma - dictionar_praguri['Transport'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Chirie' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Chirie' este",suma)
    if suma > dictionar_praguri['Chirie']:
        print("Atentie, bugetul alocat pentru categoria 'Chirie' a fost depasit cu",
              suma - dictionar_praguri['Chirie'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Utilitati' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    print("Suma totala obtinuta din factorile din categoria 'Utilitati' este", suma)
    if suma > dictionar_praguri['Utilitati']:
        print("Atentie, bugetul alocat pentru categoria 'Utilitati' a fost depasit cu",
              suma - dictionar_praguri['Utilitati'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Consumabile' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Consumabile' este",suma)
    if suma > dictionar_praguri['Consumabile']:
        print("Atentie, bugetul alocat pentru categoria 'Consumabile' a fost depasit cu",
              suma - dictionar_praguri['Consumabile'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Calatorii' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Calatorii' este",suma)
    if suma > dictionar_praguri['Calatorii']:
        print("Atentie, bugetul alocat pentru categoria 'Calatorii' a fost depasit cu",
              suma - dictionar_praguri['Calatorii'], "RON.")

    cursor.execute(""" SELECT valoare FROM factura where categorie='Diverse' """)

    data = cursor.fetchall()
    suma = 0
    for row in data:
        suma = suma + row[0]
    # print("Suma totala obtinuta din factorile din categoria 'Diverse' este",suma)
    if suma > dictionar_praguri['Diverse']:
        print("Atentie, bugetul alocat pentru categoria 'Diverse' a fost depasit cu",
              suma - dictionar_praguri['Diverse'], "RON.")


def afisareIstoric():
    """
    functia care va afisa istoricul facturilor primite
    """
    print("\n< Istoric facturi >")
    cursor.execute(""" SELECT categorie, valoare, data FROM factura """)

    data = cursor.fetchall()
    print(
        "__________________________________________________________________________________________________")

    for row in data:
        print("A fost inregistrata o factura din categoria:", row[0], "in valoare de", row[1], "RON la data", row[2],
              ";")
    print(
        "___________________________________________________________________________________________________")


dictionar_praguri = citirePraguri("PraguriCategorii.txt")
# verifDepasire(dictionar_praguri)
# exfactura = {"Furnizor": "DelGaz", "Tip": "Factura Gaz", "Data": "27/09/2020", "Categorie": "", "Valoare": 1050}
# adaugareFactura(exfactura)
afisareIstoric()

watcher = Watcher()
watcher.run()

# inchidem conexiunea cu baza de date.
if (conn):
    cursor.close()
    conn.close()
    print("PostgreSQL connection is closed")
