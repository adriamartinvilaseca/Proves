import psycopg2
import csv
import pandas as pd
from pandas import ExcelWriter


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
saed = []
conjuntRefAEG = 0
municipisSC = {}
municipis = []
resultatsMuni_AEG = {}
resultatsMuni_SAED = {}
resultatsMuni_tots = {}

with open("SeccCensals_AEG.csv") as csvfile :
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        aeg.append(i[0])

with open("SeccCensals_SAED.csv") as csvfile:
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        saed.append(i[0])

with open("SeccCensals_Municipis_totes.csv") as csvfile2:
    munireader = csv.reader(csvfile2, delimiter=";")
    next(munireader)
    for r in munireader:
        if r[1] not in municipisSC:
            municipisSC[r[1]] = [r[0]]
        else:
            municipisSC[r[1]].append(r[0])
        municipis.append(r[1])

nomsVariables = ["immb", "viv", "m2_SBR", "m2_VIV_SBR"]
ref_aeg = []
for m in municipis:
    resultatsMuni_AEG[m] = {
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
    resultatsMuni_SAED[m] = {
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
    resultatsMuni_tots[m] = {
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

immbtotal_tots = 0
vivtotal_tots = 0
immbtotal_AEG = 0
vivtotal_AEG = 0
immbtotal_SAED = 0
vivtotal_SAED = 0
count = 0
count2 = []
seccNoRepresent = []

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
    if len(r) != 20:
        r.append("")
    if r[5] != "P_CORR" and r[17] * 10 <= r[16] and r[3] != "IMMB_NO_V" and r[16] > 0 and r[19] != "":
        resultatsMuni_tots[r[19]][r[13]] += r[6]
        immbtotal_tots += 1
        vivtotal_tots += r[6]
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni_AEG[r[19]][r[13]] += r[6]
        immbtotal_AEG += 1
        vivtotal_AEG += r[6]
    if r[1] in saed and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni_SAED[r[19]][r[13]] += r[6]
        immbtotal_SAED += 1
        vivtotal_SAED += r[6]

resultatsMuniDF_tots = pd.DataFrame(resultatsMuni_tots)
resultatsMuniDF_tots = resultatsMuniDF_tots.T
resultatsMuniDF_tots = resultatsMuniDF_tots.sort_index()

resultatsMuniDF_AEG = pd.DataFrame(resultatsMuni_AEG)
resultatsMuniDF_AEG = resultatsMuniDF_AEG.T
resultatsMuniDF_AEG = resultatsMuniDF_AEG.sort_index()

resultatsMuniDF_SAED = pd.DataFrame(resultatsMuni_SAED)
resultatsMuniDF_SAED = resultatsMuniDF_SAED.T
resultatsMuniDF_SAED = resultatsMuniDF_SAED.sort_index()

print("")
print("Num immb tots", end=": ")
print(immbtotal_tots)
print("Num hab tots", end=": ")
print(vivtotal_tots)
print("")
print("Num immb AEG", end=": ")
print(immbtotal_AEG)
print("Num hab AEG", end=": ")
print(vivtotal_AEG)
print("")
print("Num immb SAED", end=": ")
print(immbtotal_SAED)
print("Num hab SAED", end=": ")
print(vivtotal_SAED)

writer = ExcelWriter("ResultatsSegMuni.xlsx", engine="xlsxwriter")
resultatsMuniDF_tots.to_excel(writer, sheet_name="Tots")
resultatsMuniDF_AEG.to_excel(writer, sheet_name="AEG")
resultatsMuniDF_SAED.to_excel(writer, sheet_name="SAED")
writer.save()