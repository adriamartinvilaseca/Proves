import psycopg2
import csv
import pandas as pd

def getReferencies(cursor):
    cursor.execute("SELECT REFCAT, SECC_CENSAL, IMMB_US_PRN, IMMB_TIPUS, IMMB_TIPUS_PERCENT, UNI_PLURI_CORR, NUM_V, "
                   "PLURI_NUM_V, ORD, ANYCONST_SUP_V, ANYCONST_ETAPA_SUP_V, AL_V_MAX, AL_IMMB, SEGMENT_100, SEGMENT_10,"
                   "SUP_SBR, SUP_VIV_SBR, SUP_VIV_IND, SUP_VIV_STR, SUP_TOTAL FROM referencies_alpha;")
    return cursor.fetchall()

def getRef(cursor, refcat):
    cursor.execute("SELECT * FROM referencies_alpha WHERE REFCAT = '{}';".format(refcat))
    return cursor.fetchall()


conn_string = "host='prodtestdb.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='dbprodtest' user='testuser' password='CICLICAc1cl1c4'"
print("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

conjuntRef = set(getReferencies(cursor))

conn.commit()
cursor.close()
conn.close()


nomsVariables = ["immb", "viv", "m2_SBR", "m2_VIV_SBR"]
nomsVariablesPlanol = ["Immb_us_prn", "Immb_tipus_resi", "Immb_tipus_prop", "Immb_num_v", "Immb_ord",
                      "Immb_anycons", "Immb_numplantes", "Cluster"]
aeg = []
conjuntRefAEG = 0
municipis = {}
error1 = []
planolRef = {}

with open("SeccCensals_AEG.csv") as csvfile:
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
        municipis[r[0]] = seccCensalsMuni

countImmb = 0
countImmb_VIV = 0
countViv = 0
countSUP_TOTAL = 0
countSUP_SBR = 0
countSUP_VIV_SBR = 0
countError = 0
countImmb_AEG = 0
countImmb_VIV_AEG = 0
countViv_AEG = 0
countSUP_TOTAL_AEG = 0
countSUP_SBR_AEG = 0
countSUP_VIV_SBR_AEG = 0
countError_AEG = 0

#################################################### Analisi variables #################################################
for r in conjuntRef:
    r = list(r)
    if r[4] is not None:
        r[4] = float(r[4])
    if r[6] is not None:
        r[6] = int(r[6])
    if r[15] is not None:
        r[15] = float(r[15])
    if r[16] is not None:
        r[16] = float(r[16])
    if r[17] is not None:
        r[17] = float(r[17])
    if r[18] is not None:
        r[18] = float(r[18])
    if r[19] is not None:
        r[19] = float(r[19])
    if r[1] in aeg:
        countImmb_AEG += 1
        if r[6] > 0:
            countImmb_VIV_AEG += 1
        countViv_AEG += r[6]
        countSUP_TOTAL_AEG += r[19]
        countSUP_SBR_AEG += r[15]
        countSUP_VIV_SBR_AEG += r[16] + r[17] + r[18]
    if r[1] in aeg and r[17] * 10 >= r[15]:
        countError_AEG += 1
    countImmb += 1
    if r[6] > 0:
        countImmb_VIV += 1
    countViv += r[6]
    countSUP_TOTAL += r[19]
    countSUP_SBR += r[15]
    countSUP_VIV_SBR += r[16]
    if r[17] * 10 >= r[15]:
        countError += 1
print("TOTAL:")
print("Immb", end=": ")
print(countImmb)
print("Immb", end=": ")
print(countImmb_VIV)
print("Viv", end=": ")
print(countViv)
print("SUP_TOTAL", end=": ")
print(countSUP_TOTAL)
print("SUP_SBR", end=": ")
print(countSUP_SBR)
print("SUP_VIV_SBR", end=": ")
print(countSUP_VIV_SBR)
print("ErrorsIndeterminat", end=": ")
print(countError)
print("")
print("AEG:")
print("Immb", end=": ")
print(countImmb_AEG)
print("Immb", end=": ")
print(countImmb_VIV_AEG)
print("Viv", end=": ")
print(countViv_AEG)
print("SUP_TOTAL", end=": ")
print(countSUP_TOTAL_AEG)
print("SUP_SBR", end=": ")
print(countSUP_SBR_AEG)
print("SUP_VIV_SBR", end=": ")
print(countSUP_VIV_SBR_AEG)
print("ErrorsIndeterminat", end=": ")
print(countError_AEG)

