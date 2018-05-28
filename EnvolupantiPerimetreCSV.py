import csv
import psycopg2


def insertgraf(cursor, m):
    print(m)
    cursor.execute("INSERT INTO GRAF_INIT (REFCAT, PLANTA, PERIMETRE, TIPUS_P, LONG) VALUES (%s, %s, %s);",m) # Revisar!


# conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
# print("Connecting to database\n    ->%s" % (conn_string))
# conn = psycopg2.connect(conn_string)
# cursor = conn.cursor()

# Organització info: {Refcat:Planta:[Area, Perimetre, Façana, Pati, Mitgera exterior, Mitgera interior]}

# Arxiu general AEG
with open("GrafAEG.csv") as csvfile1:
    perireader = csv.reader(csvfile1, delimiter=",")
    next(perireader)
    refcat = {}
    for row2 in perireader:
        id = row2[0]
        altura = row2[1]
        if id in refcat:
            refcat[id][altura] = [row2[3],row2[2],"","","",""]
        else:
            refcat[id] = {altura: [row2[3],row2[2],"","","",""]}

# Arxiu envolupants SAED
with open("GrafSAED_Barbera.csv" ) as csvfile2:
    grafreader = csv.reader(csvfile2, delimiter=",")
    next(grafreader)
    for row in grafreader:
        id = row[0]
        altura = row[1]
        tipus = row[2]
        llarg = ((float(row[5]) - float(row[7])) ** 2 + (float(row[6]) - float(row[8])) ** 2) ** 0.5
        if id in refcat:
            if altura in refcat[id]:
                if tipus == "F":
                    refcat[id][altura][2] += llarg
                elif tipus == "PI":
                    refcat[id][altura][3] += llarg
                elif tipus == "ME":
                    refcat[id][altura][4] += llarg
                else:
                    refcat[id][altura][5] += llarg
            else:
                refcat[id][altura] = ["-","-",0,0,0,0]
                if tipus == "F":
                    refcat[id][altura][2] = llarg
                elif tipus == "PI":
                    refcat[id][altura][3] = llarg
                elif tipus == "ME":
                    refcat[id][altura][4] = llarg
                else:
                    refcat[id][altura][5] = llarg
        else:
            refcat[id] = {altura:["-","-",0, 0, 0, 0]}
            if tipus == "F":
                refcat[id][altura][2] = llarg
            elif tipus == "PI":
                refcat[id][altura][3] = llarg
            elif tipus == "ME":
                refcat[id][altura][4] = llarg
            else:
                refcat[id][altura][5] = llarg

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