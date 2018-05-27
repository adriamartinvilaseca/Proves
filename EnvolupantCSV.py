import csv
import psycopg2


def insertgraf(cursor, m):
    print(m)
    cursor.execute("INSERT INTO GRAF_INIT (REFCAT, PLANTA, PERIMETRE, TIPUS_P, LONG) VALUES (%s, %s, %s);",m)




conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
print("Connecting to database\n    ->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# with open("GrafAEG.csv") as csvfile :
    # ordreader = csv.reader(csvfile, delimiter=";")
    # next(ordreader)
    # for i in ordreader:
        # m = (i[0], i[1], i[2], i[3])
        # insertgraf(cursor, m)

with open("GrafSAED_Barbera.csv" ) as csvfile :
    grafreader = csv.reader(csvfile, delimiter=",")
    next(grafreader)
    for i in grafreader:
        a = (float(i[5])-float(i[7]))**2+(float(i[6])-float(i[8]))**2)**0.5
        m = (i[0], i[1], i[2], a)
        print(m)
        # insertord(cursor, m)

conn.commit()
cursor.close()
conn.close()