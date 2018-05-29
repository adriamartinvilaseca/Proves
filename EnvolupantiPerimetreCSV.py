import csv
# import psycopg2
from shapely.geometry import Polygon, LineString
from matplotlib import pyplot as plt

def insertgraf(cursor, m):
    print(m)
    cursor.execute("INSERT INTO GRAF_INIT (REFCAT, PLANTA, PERIMETRE, TIPUS_P, LONG) VALUES (%s, %s, %s);",m)  # Revisar!


# conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
# print("Connecting to database\n    ->%s" % (conn_string))
# conn = psycopg2.connect(conn_string)
# cursor = conn.cursor()

# Organització info: {Refcat:Planta:[Area, Perimetre, Façana, Pati, Mitgera exterior, Mitgera interior]}


# Arxiu envolupants SAED
with open("GrafSAED_Barbera.csv") as csvfile2:
    grafreader = csv.reader(csvfile2, delimiter=",")
    next(grafreader)
    refcat = {}
    for row in grafreader:
        id = row[0]
        altura = row[1]
        tipus = row[2]
        llarg = ((float(row[5]) - float(row[7])) ** 2 + (float(row[6]) - float(row[8])) ** 2) ** 0.5
        inici = (float(row[5]), float(row[6]))
        final = (float(row[7]), float(row[8]))
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
                refcat[id][altura] = ["-", "-", 0, 0, 0, 0, []]
                if tipus == "F":
                    refcat[id][altura][2] = llarg
                elif tipus == "PI":
                    refcat[id][altura][3] = llarg
                elif tipus == "ME":
                    refcat[id][altura][4] = llarg
                else:
                    refcat[id][altura][5] = llarg
        else:
            refcat[id] = {altura:["-", "-", 0, 0, 0, 0, []]}
            if tipus == "F":
                refcat[id][altura][2] = llarg
            elif tipus == "PI":
                refcat[id][altura][3] = llarg
            elif tipus == "ME":
                refcat[id][altura][4] = llarg
            else:
                refcat[id][altura][5] = llarg
        refcat[id][altura][6].append(inici)
        refcat[id][altura][6].append(final)
        # refcat[id][altura][6].append(LineString([inici, final]))
    for id in refcat:
        for altura in refcat[id]:
            refcat[id][altura][1] = refcat[id][altura][2]+refcat[id][altura][3]+refcat[id][altura][4]+refcat[id][altura][5]
            # poli = Polygon(refcat[id][altura][6])
            # refcat[id][altura][0] = poli.area  # Falta treure els patis interiors
            poli = LineString(refcat[id][altura][6]).convex_hull
            refcat[id][altura][0] = poli.area
            fig = plt.figure()
            ax = fig.add_subplot(111)
            x, y = poli.exterior.xy
            ax.plot(x, y, color="#6699cc", alpha=0.7, linewidth=3, solid_capstyle="round", zorder=2)
            print(id)
            print(altura)
            print(poli)
            plt.show()
            a = input()


# Arxiu general AEG
with open("GrafAEG.csv") as csvfile1:
    perireader = csv.reader(csvfile1, delimiter=",")
    next(perireader)
    for row2 in perireader:
        id = row2[0]
        altura = row2[1]
        if id not in refcat:
            refcat[id] = {altura: [row2[3], row2[2], "", "", "", ""]}
        # else:
            # if altura in refcat[id]:
            # refcat[id][altura][0] = row2[3]
            # refcat[id][altura][1] = row2[2]
            # else:
            # refcat[id][altura] = [row2[3], row2[2], 0, 0, 0, 0]


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