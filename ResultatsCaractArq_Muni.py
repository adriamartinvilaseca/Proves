import psycopg2
import csv
import pandas as pd


def getReferencies(cursor):
    cursor.execute("SELECT REFCAT, SECC_CENSAL, IMMB_US_PRN, IMMB_TIPUS, IMMB_TIPUS_PERCENT, UNI_PLURI_CORR, NUM_V, "
                   "PLURI_NUM_V, ORD, ANYCONST_SUP_V, ANYCONST_ETAPA_SUP_V, AL_V_MAX, AL_IMMB, SEGMENT_100, SEGMENT_10,"
                   "SUP_SBR, SUP_VIV_SBR FROM referencies_alpha;")
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

nomsVariables = ["viv"]
aeg = []
conjuntRefAEG = 0
municipisSC = {}
municipis = []
resultatsMuni = {}

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
        municipisSC[r[0]] = seccCensalsMuni
        municipis.append(r[0])

#################################################### Definicio diccionaris #############################################
for m in municipis:
    resultatsMuni[m] = {
        "Residencial": 0,
        "Comercial": 0,
        "Oficines": 0,
        "HotelerRestauracio": 0,
        "Public": 0,
        "EnsenyamentCultural": 0,
        "Esportiu": 0,
        "Piscines": 0,
        "Industrial": 0,
        "IndustrialResta": 0,
        "Aparcament": 0,
        "Altres": 0,
        "Error": 0,
        "Total_immb_us_prn": 0,
        "IMMB_EXC_V": 0,
        "IMMB_PRN_75_V": 0,
        "IMMB_PRN_50_V": 0,
        "IMMB_AMB_V": 0,
        "IMMB_NO_V": 0,
        "Total_immb_tipus_resi": 0,
        "DivisioHor": 0,
        "NoDivisioHor": 0,
        "Total_immb_tipus_prop": 0,
        "U": 0,
        "P_2a4": 0,
        "P_5a9": 0,
        "P_10a19": 0,
        "P_20a39": 0,
        "P_40mes": 0,
        "Total_num_v": 0,
        "EAI": 0,
        "EAV": 0,
        "EVE": 0,
        "EFI": 0,
        "IND": 0,
        "Total_Ord": 0,
        "FS35": 0,
        "3660": 0,
        "6180": 0,
        "8107": 0,
        "08EN": 0,
        "Total_anycons": 0,
        "De P-1 en avall": 0,
        "De PB a PB+2": 0,
        "De PB+3 o mes": 0,
        "IND_alt": 0,
        "Total_immb_numplantes": 0
    }

#################################################### Analisi variables #################################################
for r in conjuntRef:
    r = list(r)
    for a in municipisSC:
        for b in municipisSC[a]:
            if r[1] == b:
                r.append(a)
    if r[4] is not None:
        r[4] = float(r[4])
    if r[6] is not None:
        r[6] = int(r[6])
    if r[15] is not None:
        r[15] = float(r[15])
    if r[16] is not None:
        r[16] = float(r[16])

#################################################### Immb_us_prn #######################################################
    if r[1] in aeg and r[5] != "P_CORR":
        if r[2] == "AltresCal" or r[2] == "AltresNoCal" or r[2] == "Emmagatzematge":
            resultatsMuni[r[17]]["Altres"] += r[6]
        elif r[2] != "Comu" and r[2] != "VincViv":
            resultatsMuni[r[17]][r[2]] += r[6]
        resultatsMuni[r[17]]["Total_immb_us_prn"] += r[6]

#################################################### Immb_tipus_resi ###################################################
    if r[1] in aeg and r[5] != "P_CORR":
        resultatsMuni[r[17]][r[3]] += r[6]
        resultatsMuni[r[17]]["Total_immb_tipus_resi"] += r[6]

#################################################### Immb_tipus_prop ###################################################
    if r[1] in aeg and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[5] == "U" or r[5] == "P_CORR":
            resultatsMuni[r[17]]["NoDivisioHor"] += r[6]
        elif r[5] == "P":
            resultatsMuni[r[17]]["DivisioHor"] += r[6]
        resultatsMuni[r[17]]["Total_immb_tipus_prop"] += r[6]

#################################################### Immb_num_v ########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        resultatsMuni[r[17]][r[7]] += r[6]
        resultatsMuni[r[17]]["Total_num_v"] += r[6]

#################################################### Immb_ord ##########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[8] == "IND" or r[8] == "NO_V":
            resultatsMuni[r[17]]["IND"] += r[6]
        else:
            resultatsMuni[r[17]][r[8]] += r[6]
        resultatsMuni[r[17]]["Total_Ord"] += r[6]

#################################################### Immb_anycons ######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        resultatsMuni[r[17]][r[10]] += r[6]
        resultatsMuni[r[17]]["Total_anycons"] += r[6]

#################################################### Immb_numplantes ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        resultatsMuni[r[17]][r[12]] += r[6]
        resultatsMuni[r[17]]["Total_immb_numplantes"] += r[6]

################################################ PRINT #################################################################
print("")
print("Total referències: ", end=": ")
print(len(conjuntRef))
print("Total referències àmbit: ", end=": ")
print(conjuntRefAEG)
print("")

# resultatsMuniDF = pd.DataFrame(resultatsMuni)
# resultatsMuniDF.index = municipis
# resultatsMuniDF = resultatsMuniDF.T

for a in resultatsMuni:
    keys = list(resultatsMuni[a].keys())
    resultats = list(resultatsMuni[a].values())
    resultats.insert(0, a)
    print(resultats)
print(keys)
# provaDF = immb_us_prnDF + immb_tipus_resiDF + immb_tipus_propDF
# resultatsMuniDF.to_csv("ResultatsArqMuni.csv")
