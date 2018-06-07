import csv
import psycopg2
from shapely.geometry import Polygon, LineString
from matplotlib import pyplot as plt

def insertgraf(cursor, m):
    print(m)
    cursor.execute("INSERT INTO GRAF_INIT (PLANTA, REFCAT, ML_EN_VER, SUP_HOR_GRAF, ML_EN_VER_F, ML_EN_VER_P, "
                   "ML_EN_VER_ME, ML_EN_VER_MI) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",m)  # Revisar!


# Organització info: {Refcat:Planta:[Area, Perimetre, Façana, Pati, Mitgera exterior, Mitgera interior]}

refcat = {}

# Lectura arxiu general AEG
with open("SAEND_PerimArees.csv") as csvfile1:
    perireader = csv.reader(csvfile1, delimiter=",")
    next(perireader)
    for row2 in perireader:
        id = row2[0]
        altura = row2[1]
        if id not in refcat:
            refcat[id] = {altura: [float(row2[3]), float(row2[2]), 0, 0, 0, 0]}
        else:
            if altura in refcat[id]:
                refcat[id][altura][0] += float(row2[3])
                refcat[id][altura][1] += float(row2[2])
            else:
                refcat[id][altura] = [float(row2[3]), float(row2[2]), 0, 0, 0, 0]

# Lectura arxiu general AEG
with open("SAED_PerimArees.csv") as csvfile3:
    perireader2 = csv.reader(csvfile3, delimiter=",")
    next(perireader2)
    for row3 in perireader2:
        id = row3[0]
        altura = row3[1]
        if id not in refcat:
            refcat[id] = {altura: [float(row3[3]), float(row3[2]), 0, 0, 0, 0]}
        else:
            if altura in refcat[id]:
                refcat[id][altura][0] += float(row3[3])
                refcat[id][altura][1] += float(row3[2])
            else:
                refcat[id][altura] = [float(row3[3]), float(row3[2]), 0, 0, 0, 0]

# Lectura axiu envolupants SAED
with open("SAED_parets.csv") as csvfile2:
    grafreader = csv.reader(csvfile2, delimiter=",")
    next(grafreader)
    for row in grafreader:
        id = row[0]
        altura = str(row[1])
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
                refcat[id][altura] = ["-", "-", 0, 0, 0, 0,]
                if tipus == "F":
                    refcat[id][altura][2] = llarg
                elif tipus == "PI":
                    refcat[id][altura][3] = llarg
                elif tipus == "ME":
                    refcat[id][altura][4] = llarg
                else:
                    refcat[id][altura][5] = llarg
        else:
            refcat[id] = {altura:["-", "-", 0, 0, 0, 0,]}
            if tipus == "F":
                refcat[id][altura][2] = llarg
            elif tipus == "PI":
                refcat[id][altura][3] = llarg
            elif tipus == "ME":
                refcat[id][altura][4] = llarg
            else:
                refcat[id][altura][5] = llarg

# errors = [0]
# errorsRef = []
# for id in refcat.keys():
#     for altura in refcat[id]:
#         peri = refcat[id][altura][2] + refcat[id][altura][3] + refcat[id][altura][4] + refcat[id][altura][5]
#         if refcat[id][altura][1] != "-" and peri != 0:
#             peri2 = float(refcat[id][altura][1])
#             if abs(peri - peri2) > 1:
#                 errors[0] += 1
#                 referror = id + " " + altura + " " + str(peri) + " " + str(peri2)
#                 errorsRef.append(referror)
#                 print(referror)
#
# print("")
# print("Num de casos amb més de 10m de dif", end=": ")
# print(len(errorsRef))

# Pujar a la BBDD
conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
print("Connecting to database\n    ->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

conjuntInfoBBDD = []
for m in refcat.keys():
    for altura in refcat[m]:
        infoBBDD = [int(altura)-1, m, refcat[m][altura][1], refcat[m][altura][0], refcat[m][altura][2],
                    refcat[m][altura][3], refcat[m][altura][4], refcat[m][altura][5]]
        # print(infoBBDD)
        insertgraf(cursor, infoBBDD)

conn.commit()
cursor.close()
conn.close()