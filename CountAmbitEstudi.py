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
count = 0

#################################################### Analisi variables #################################################
for r in conjuntRef:
    r = list(r)
    if r[1] in aeg:
        conjuntRefAEG += 1