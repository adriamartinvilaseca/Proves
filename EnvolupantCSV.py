import csv
import psycopg2


def insertgraf(cursor, m):
    print(m)
    cursor.execute("INSERT INTO GRAF_INIT (REFCAT, PLANTA, PERIMETRE, TIPUS_P, LONG) VALUES (%s, %s, %s);",m)


# conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
# print("Connecting to database\n    ->%s" % (conn_string))
# conn = psycopg2.connect(conn_string)
# cursor = conn.cursor()

# Arxiu general AEG
# with open("GrafAEG.csv") as csvfile :
    # ordreader = csv.reader(csvfile, delimiter=";")
    # next(ordreader)
    # refcat = {}
    # for row2 in ordreader:
        # id = row[0]
        # altura = row[1]
        # if id in refcat:
            # if altura in refcat[id]
                # refcat[id][altura] = [row[2],row[3],]
        # m = (i[0], i[1], i[2], i[3])
        # insertgraf(cursor, m)

# Arxiu envolupants SAED
with open("GrafSAED_Barbera.csv" ) as csvfile :
    grafreader = csv.reader(csvfile, delimiter=",")
    next(grafreader)
    refcat = {}
    for row in grafreader:
        id = row[0]
        altura = row[1]
        tipus = row[2]
        llarg = ((float(row[5]) - float(row[7])) ** 2 + (float(row[6]) - float(row[8])) ** 2) ** 0.5
        if id in refcat:
            if altura in refcat[id]:
                if tipus == "F":
                    refcat[id][altura][0] += llarg
                elif tipus == "PI":
                    refcat[id][altura][1] += llarg
                elif tipus == "ME":
                    refcat[id][altura][2] += llarg
                else:
                    refcat[id][altura][3] += llarg
            else:
                refcat[id][altura] = [0,0,0,0]
                if tipus == "F":
                    refcat[id][altura][0] = llarg
                elif tipus == "PI":
                    refcat[id][altura][1] = llarg
                elif tipus == "ME":
                    refcat[id][altura][2] = llarg
                else:
                    refcat[id][altura][3] = llarg
        else:
            refcat[id] = {altura:[0, 0, 0, 0]}
            if tipus == "F":
                refcat[id][altura][0] = llarg
            elif tipus == "PI":
                refcat[id][altura][1] = llarg
            elif tipus == "ME":
                refcat[id][altura][2] = llarg
            else:
                refcat[id][altura][3] = llarg

# Pujar a la BBDD
# insertord(cursor, m)

# Revisió resultats
for id in refcat:
    print("")
    print(id)
    for altura in refcat[id]:
        print(altura, end=": ")
        print(refcat[id][altura])

print("")
print("Num registres diccionari: ", end=": ")
print(len(refcat.keys()))


# conn.commit()
# cursor.close()
# conn.close()