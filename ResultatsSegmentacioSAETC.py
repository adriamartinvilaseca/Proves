import psycopg2
import csv
import pandas as pd

def getReferencies(cursor):
    cursor.execute("SELECT REFCAT, SECC_CENSAL, IMMB_US_PRN, IMMB_TIPUS, IMMB_TIPUS_PERCENT, UNI_PLURI_CORR, NUM_V, "
                   "PLURI_NUM_V, ORD, ANYCONST_SUP_V, ANYCONST_ETAPA_SUP_V, AL_V_MAX, AL_IMMB, SEGMENT_100, SEGMENT_10,"
                   "SUP_SBR, SUP_VIV_SBR, SUP_VIV_IND, SUP_VIV_STR FROM referencies_alpha;")
    return cursor.fetchall()

def getRef(cursor, refcat):
    cursor.execute("SELECT * FROM referencies_alpha WHERE REFCAT = '{}';".format(refcat))
    return cursor.fetchall()

conn_string = "host='prodtestdb.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='dbprodtest' user='testuser' password='CICLICAc1cl1c4'"
print("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

conjuntRA = set(getReferencies(cursor))

conn.commit()
cursor.close()
conn.close()


nomsVariables = ["immb", "viv", "m2_SBR", "m2_VIV_SBR"]
saetc = []
ref_aeg = []
segmentacio100 = {
    "A.1.1": [0, 0, 0, 0], "A.1.2": [0, 0, 0, 0], "A.1.3": [0, 0, 0, 0], "A.1.4": [0, 0, 0, 0], "A.1.5": [0, 0, 0, 0], "A.2.1": [0, 0, 0, 0], "A.2.2": [0, 0, 0, 0], "A.2.3": [0, 0, 0, 0], "A.2.4": [0, 0, 0, 0], "A.2.5": [0, 0, 0, 0],
    "B.1.1": [0, 0, 0, 0], "B.1.2": [0, 0, 0, 0], "B.1.3": [0, 0, 0, 0], "B.1.4": [0, 0, 0, 0], "B.1.5": [0, 0, 0, 0], "B.2.1": [0, 0, 0, 0], "B.2.2": [0, 0, 0, 0], "B.2.3": [0, 0, 0, 0], "B.2.4": [0, 0, 0, 0], "B.2.5": [0, 0, 0, 0],
    "C.1.1": [0, 0, 0, 0], "C.1.2": [0, 0, 0, 0], "C.1.3": [0, 0, 0, 0], "C.1.4": [0, 0, 0, 0], "C.1.5": [0, 0, 0, 0], "C.2.1": [0, 0, 0, 0], "C.2.2": [0, 0, 0, 0], "C.2.3": [0, 0, 0, 0], "C.2.4": [0, 0, 0, 0], "C.2.5": [0, 0, 0, 0],
    "D.1.1": [0, 0, 0, 0], "D.1.2": [0, 0, 0, 0], "D.1.3": [0, 0, 0, 0], "D.1.4": [0, 0, 0, 0], "D.1.5": [0, 0, 0, 0], "D.2.1": [0, 0, 0, 0], "D.2.2": [0, 0, 0, 0], "D.2.3": [0, 0, 0, 0], "D.2.4": [0, 0, 0, 0], "D.2.5": [0, 0, 0, 0],
    "E.1.1": [0, 0, 0, 0], "E.1.2": [0, 0, 0, 0], "E.1.3": [0, 0, 0, 0], "E.1.4": [0, 0, 0, 0], "E.1.5": [0, 0, 0, 0], "E.2.1": [0, 0, 0, 0], "E.2.2": [0, 0, 0, 0], "E.2.3": [0, 0, 0, 0], "E.2.4": [0, 0, 0, 0], "E.2.5": [0, 0, 0, 0],
    "F.1.1": [0, 0, 0, 0], "F.1.2": [0, 0, 0, 0], "F.1.3": [0, 0, 0, 0], "F.1.4": [0, 0, 0, 0], "F.1.5": [0, 0, 0, 0], "F.2.1": [0, 0, 0, 0], "F.2.2": [0, 0, 0, 0], "F.2.3": [0, 0, 0, 0], "F.2.4": [0, 0, 0, 0], "F.2.5": [0, 0, 0, 0],
    "G.1.1": [0, 0, 0, 0], "G.1.2": [0, 0, 0, 0], "G.1.3": [0, 0, 0, 0], "G.1.4": [0, 0, 0, 0], "G.1.5": [0, 0, 0, 0], "G.2.1": [0, 0, 0, 0], "G.2.2": [0, 0, 0, 0], "G.2.3": [0, 0, 0, 0], "G.2.4": [0, 0, 0, 0], "G.2.5": [0, 0, 0, 0],
    "H.1.1": [0, 0, 0, 0], "H.1.2": [0, 0, 0, 0], "H.1.3": [0, 0, 0, 0], "H.1.4": [0, 0, 0, 0], "H.1.5": [0, 0, 0, 0], "H.2.1": [0, 0, 0, 0], "H.2.2": [0, 0, 0, 0], "H.2.3": [0, 0, 0, 0], "H.2.4": [0, 0, 0, 0], "H.2.5": [0, 0, 0, 0],
    "I.1.1": [0, 0, 0, 0], "I.1.2": [0, 0, 0, 0], "I.1.3": [0, 0, 0, 0], "I.1.4": [0, 0, 0, 0], "I.1.5": [0, 0, 0, 0], "I.2.1": [0, 0, 0, 0], "I.2.2": [0, 0, 0, 0], "I.2.3": [0, 0, 0, 0], "I.2.4": [0, 0, 0, 0], "I.2.5": [0, 0, 0, 0],
    "J.1.1": [0, 0, 0, 0], "J.1.2": [0, 0, 0, 0], "J.1.3": [0, 0, 0, 0], "J.1.4": [0, 0, 0, 0], "J.1.5": [0, 0, 0, 0], "J.2.1": [0, 0, 0, 0], "J.2.2": [0, 0, 0, 0], "J.2.3": [0, 0, 0, 0], "J.2.4": [0, 0, 0, 0], "J.2.5": [0, 0, 0, 0]
}

with open("SeccCensals_SAETC.csv") as csvfile :
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        saetc.append(i[0])

immbtotal = 0
vivtotal = 0

for r in conjuntRA:
    r = list(r)
    if r[4] is not None:
        r[4] = float(r[4])
    if r[15] is not None:
        r[15] = float(r[15])
    if r[16] is not None:
        r[16] = float(r[16])
    if r[17] is not None:
        r[17] = float(r[17])
    if r[18] is not None:
        r[18] = float(r[18])
    if r[6] is not None and r[16] > 0:
        r[6] = int(r[6])
    else:
        r[6] = 0

    if r[1] in saetc and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        segmentacio100[r[13]][0] += 1
        segmentacio100[r[13]][1] += r[6]
        segmentacio100[r[13]][2] += r[15]
        segmentacio100[r[13]][3] += r[16]
        immbtotal += 1
        vivtotal += r[6]

segmentacio100DF = pd.DataFrame(segmentacio100)
segmentacio100DF.index = nomsVariables
print("")
print("Segmentacio 100: ")
print(segmentacio100DF)
segmentacio100DF.to_csv("Segmentacio100_SAETC.csv")

print("")
print("Num immb", end=": ")
print(immbtotal)
print("Num hab", end=": ")
print(vivtotal)

