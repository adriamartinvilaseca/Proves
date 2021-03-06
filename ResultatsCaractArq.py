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
#################################################### Definicio diccionaris #############################################
immb_us_prn = {
    "Residencial": [0, 0, 0, 0],
    "Comercial": [0, 0, 0, 0],
    "Oficines": [0, 0, 0, 0],
    "HotelerRestauracio": [0, 0, 0, 0],
    "Public": [0, 0, 0, 0],
    "EnsenyamentCultural": [0, 0, 0, 0],
    "Esportiu": [0, 0, 0, 0],
    "Piscines": [0, 0, 0, 0],
    "Industrial": [0, 0, 0, 0],
    "IndustrialResta": [0, 0, 0, 0],
    "Aparcament": [0, 0, 0, 0],
    "Altres": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_tipus_resi = {
    "IMMB_EXC_V": [0, 0, 0, 0],
    "IMMB_PRN_75_V": [0, 0, 0, 0],
    "IMMB_PRN_50_V": [0, 0, 0, 0],
    "IMMB_AMB_V": [0, 0, 0, 0],
    "IMMB_NO_V": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_tipus_prop = {
    "DivisioHor": [0, 0, 0, 0],
    "NoDivisioHor": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_num_v = {
    "U": [0, 0, 0, 0],
    "P_2a4": [0, 0, 0, 0],
    "P_5a9": [0, 0, 0, 0],
    "P_10a19": [0, 0, 0, 0],
    "P_20a39": [0, 0, 0, 0],
    "P_40mes": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_ord = {
    "EAI": [0, 0, 0, 0],
    "EAV": [0, 0, 0, 0],
    "EVE": [0, 0, 0, 0],
    "EFI": [0, 0, 0, 0],
    "IND": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_anycons = {
    "FS35": [0, 0, 0, 0],
    "3660": [0, 0, 0, 0],
    "6180": [0, 0, 0, 0],
    "8107": [0, 0, 0, 0],
    "08EN": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}
immb_numplantes = {
    "De PB a PB+2": [0, 0, 0, 0],
    "De PB+3 o mes": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}

#################################################### Analisi variables #################################################
for r in conjuntRef:
    r = list(r)
    if r[1] in aeg:
        conjuntRefAEG += 1
        planolRef[r[0]] = ["", "", "", "", "", "", "", ""] # Refcat, Immb_us_prn, Immb_tipus_resi, Immb_tipus_prop, Immb_num_v, Immb_ord, Immb_anycons, Immb_numplantes, Cluster
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

#################################################### Immb_us_prn #######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[17]*10 <= r[16] and ((r[3] != "IMMB_NO_V" and r[16] > 0) or
                                                                   r[3] == "IMMB_NO_V"):
        if r[2] == "AltresCal" or r[2] == "AltresNoCal" or r[2] == "Emmagatzematge" or r[2] == "Comu" or \
                r[2] == "VincViv":
            immb_us_prn["Altres"][0] += 1
            immb_us_prn["Altres"][1] += r[6]
            immb_us_prn["Altres"][2] += r[15]
            immb_us_prn["Altres"][3] += r[16]
            planolRef[r[0]][0] = "Altres"
        else:
            immb_us_prn[r[2]][0] += 1
            immb_us_prn[r[2]][1] += r[6]
            immb_us_prn[r[2]][2] += r[15]
            immb_us_prn[r[2]][3] += r[16]
            planolRef[r[0]][0] = r[2]
        immb_us_prn["Total"][0] += 1
        immb_us_prn["Total"][1] += r[6]
        immb_us_prn["Total"][2] += r[15]
        immb_us_prn["Total"][3] += r[16]

#################################################### Immb_tipus_resi ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[17]*10 <= r[16]:
        if (r[3] != "IMMB_NO_V" and r[16] > 0) or r[3] == "IMMB_NO_V":
            immb_tipus_resi[r[3]][0] += 1
            immb_tipus_resi[r[3]][1] += r[6]
            immb_tipus_resi[r[3]][2] += r[15]
            immb_tipus_resi[r[3]][3] += r[16]
            planolRef[r[0]][1] = r[3]
            immb_tipus_resi["Total"][0] += 1
            immb_tipus_resi["Total"][1] += r[6]
            immb_tipus_resi["Total"][2] += r[15]
            immb_tipus_resi["Total"][3] += r[16]

#################################################### Immb_tipus_prop ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        if r[5] == "U":
            immb_tipus_prop["NoDivisioHor"][0] += 1
            immb_tipus_prop["NoDivisioHor"][1] += r[6]
            immb_tipus_prop["NoDivisioHor"][2] += r[15]
            immb_tipus_prop["NoDivisioHor"][3] += r[16]
            planolRef[r[0]][2] = "NoDivisioHor"
        elif r[5] == "P":
            immb_tipus_prop["DivisioHor"][0] += 1
            immb_tipus_prop["DivisioHor"][1] += r[6]
            immb_tipus_prop["DivisioHor"][2] += r[15]
            immb_tipus_prop["DivisioHor"][3] += r[16]
            planolRef[r[0]][2] = "DivisioHor"
        immb_tipus_prop["Total"][0] += 1
        immb_tipus_prop["Total"][1] += r[6]
        immb_tipus_prop["Total"][2] += r[15]
        immb_tipus_prop["Total"][3] += r[16]

#################################################### Immb_num_v ########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17]*10 <= r[16]:
        immb_num_v[r[7]][0] += 1
        immb_num_v[r[7]][1] += r[6]
        immb_num_v[r[7]][2] += r[15]
        immb_num_v[r[7]][3] += r[16]
        planolRef[r[0]][3] = r[7]
        immb_num_v["Total"][0] += 1
        immb_num_v["Total"][1] += r[6]
        immb_num_v["Total"][2] += r[15]
        immb_num_v["Total"][3] += r[16]

#################################################### Immb_ord ##########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17]*10 <= r[16]:
        if r[8] == "IND" or r[8] == "NO_V":
            immb_ord["IND"][0] += 1
            immb_ord["IND"][1] += r[6]
            immb_ord["IND"][2] += r[15]
            immb_ord["IND"][3] += r[16]
            planolRef[r[0]][4] = "IND"
        else:
            immb_ord[r[8]][0] += 1
            immb_ord[r[8]][1] += r[6]
            immb_ord[r[8]][2] += r[15]
            immb_ord[r[8]][3] += r[16]
            planolRef[r[0]][4] = r[8]
        immb_ord["Total"][0] += 1
        immb_ord["Total"][1] += r[6]
        immb_ord["Total"][2] += r[15]
        immb_ord["Total"][3] += r[16]

#################################################### Immb_anycons ######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17]*10 <= r[16]:
        immb_anycons[r[10]][0] += 1
        immb_anycons[r[10]][1] += r[6]
        immb_anycons[r[10]][2] += r[15]
        immb_anycons[r[10]][3] += r[16]
        planolRef[r[0]][5] = r[10]
        immb_anycons["Total"][0] += 1
        immb_anycons["Total"][1] += r[6]
        immb_anycons["Total"][2] += r[15]
        immb_anycons["Total"][3] += r[16]

#################################################### Immb_numplantes ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17]*10 <= r[16]:
        immb_numplantes[r[12]][0] += 1
        immb_numplantes[r[12]][1] += r[6]
        immb_numplantes[r[12]][2] += r[15]
        immb_numplantes[r[12]][3] += r[16]
        planolRef[r[0]][6] = r[12]
        planolRef[r[0]][7] = r[14]
        immb_numplantes["Total"][0] += 1
        immb_numplantes["Total"][1] += r[6]
        immb_numplantes["Total"][2] += r[15]
        immb_numplantes["Total"][3] += r[16]

#################################################### PRINT #############################################################
print("")
print("Total referències: ", end=": ")
print(len(conjuntRef))
print("Total referències àmbit: ", end=": ")
print(conjuntRefAEG)
print("")
immb_us_prnDF = pd.DataFrame(immb_us_prn)
immb_us_prnDF.index = nomsVariables
immb_us_prnDF = immb_us_prnDF.T
print(immb_us_prnDF)
print("")
immb_tipus_resiDF = pd.DataFrame(immb_tipus_resi)
immb_tipus_resiDF.index = nomsVariables
immb_tipus_resiDF = immb_tipus_resiDF.T
print(immb_tipus_resiDF)
print("")
immb_tipus_propDF = pd.DataFrame(immb_tipus_prop)
immb_tipus_propDF.index = nomsVariables
immb_tipus_propDF = immb_tipus_propDF.T
print(immb_tipus_propDF)
print("")
immb_num_vDF = pd.DataFrame(immb_num_v)
immb_num_vDF.index = nomsVariables
immb_num_vDF = immb_num_vDF.T
print(immb_num_vDF)
print("")
immb_ordDF = pd.DataFrame(immb_ord)
immb_ordDF.index = nomsVariables
immb_ordDF = immb_ordDF.T
print(immb_ordDF)
print("")
immb_anyconsDF = pd.DataFrame(immb_anycons)
immb_anyconsDF.index = nomsVariables
immb_anyconsDF = immb_anyconsDF.T
print(immb_anyconsDF)
print("")
immb_numplantesDF = pd.DataFrame(immb_numplantes)
immb_numplantesDF.index = nomsVariables
immb_numplantesDF = immb_numplantesDF.T
print(immb_numplantesDF)

planolRefDF = pd.DataFrame(planolRef)
planolRefDF.index = nomsVariablesPlanol
planolRefDF = planolRefDF.T
planolRefDF.to_csv("CaractArq_Planol.csv")
