import psycopg2
import csv
import copy
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

aeg = []
conjuntRefAEG = 0
municipisSC = {}
municipis = []
resultatsMuni = {}

with open("SeccCensals_AEG.csv") as csvfile :
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        aeg.append(i[0])

with open("SeccCensals_Municipis.csv") as csvfile2:
    munireader = csv.reader(csvfile2, delimiter=";")
    for r in munireader:
        seccCensalsMuni = []
        for a in r[1:]:
            if a != "":
                seccCensalsMuni.append(a)
        municipisSC[r[0]] = seccCensalsMuni
        municipis.append(r[0])

nomsVariables = ["immb", "viv", "m2_SBR", "m2_VIV_SBR"]
ref_aeg = []
for m in municipis:
    resultatsMuni[m] = {
        "A.1.1": 0, "A.1.2": 0, "A.1.3": 0, "A.1.4": 0, "A.1.5": 0, "A.2.1": 0, "A.2.2": 0, "A.2.3": 0, "A.2.4": 0, "A.2.5": 0,
        "B.1.1": 0, "B.1.2": 0, "B.1.3": 0, "B.1.4": 0, "B.1.5": 0, "B.2.1": 0, "B.2.2": 0, "B.2.3": 0, "B.2.4": 0, "B.2.5": 0,
        "C.1.1": 0, "C.1.2": 0, "C.1.3": 0, "C.1.4": 0, "C.1.5": 0, "C.2.1": 0, "C.2.2": 0, "C.2.3": 0, "C.2.4": 0, "C.2.5": 0,
        "D.1.1": 0, "D.1.2": 0, "D.1.3": 0, "D.1.4": 0, "D.1.5": 0, "D.2.1": 0, "D.2.2": 0, "D.2.3": 0, "D.2.4": 0, "D.2.5": 0,
        "E.1.1": 0, "E.1.2": 0, "E.1.3": 0, "E.1.4": 0, "E.1.5": 0, "E.2.1": 0, "E.2.2": 0, "E.2.3": 0, "E.2.4": 0, "E.2.5": 0,
        "F.1.1": 0, "F.1.2": 0, "F.1.3": 0, "F.1.4": 0, "F.1.5": 0, "F.2.1": 0, "F.2.2": 0, "F.2.3": 0, "F.2.4": 0, "F.2.5": 0,
        "G.1.1": 0, "G.1.2": 0, "G.1.3": 0, "G.1.4": 0, "G.1.5": 0, "G.2.1": 0, "G.2.2": 0, "G.2.3": 0, "G.2.4": 0, "G.2.5": 0,
        "H.1.1": 0, "H.1.2": 0, "H.1.3": 0, "H.1.4": 0, "H.1.5": 0, "H.2.1": 0, "H.2.2": 0, "H.2.3": 0, "H.2.4": 0, "H.2.5": 0,
        "I.1.1": 0, "I.1.2": 0, "I.1.3": 0, "I.1.4": 0, "I.1.5": 0, "I.2.1": 0, "I.2.2": 0, "I.2.3": 0, "I.2.4": 0, "I.2.5": 0,
        "J.1.1": 0, "J.1.2": 0, "J.1.3": 0, "J.1.4": 0, "J.1.5": 0, "J.2.1": 0, "J.2.2": 0, "J.2.3": 0, "J.2.4": 0, "J.2.5": 0
    }

immbtotal = 0
vivtotal = 0

for r in conjuntRA:
    r = list(r)
    for a in municipisSC:
        for b in municipisSC[a]:
            if r[1] == b:
                r.append(a)
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

    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni[r[19]][r[13]] += r[6]
        immbtotal += 1
        vivtotal += r[6]

# for a in resultatsMuni:
#     keys = list(resultatsMuni[a].keys())
#     resultats = list(resultatsMuni[a].values())
#     resultats.insert(0, a)
#     print(resultats)
# print(keys)

resultatsMuniDF = pd.DataFrame(resultatsMuni)
# resultatsMuniDF.index = municipis
resultatsMuniDF = resultatsMuniDF.T
print(resultatsMuniDF)
resultatsMuniDF.to_csv("ResultatsSegMuni.csv")

print("")
print("Num immb: ", end=": ")
print(immbtotal)
print("Num hab: ", end=": ")
print(vivtotal)
