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

conjuntRef = set(getReferencies(cursor))

conn.commit()
cursor.close()
conn.close()

aeg = []
conjuntRefAEG = 0
municipisSC = {}
municipis = []
resultatsMuni_immb = {}
resultatsMuni_viv = {}
resultatsMuni_SupVivSBR = {}
resultatsMuni_SupSBR = {}

with open("SeccCensals_AEG.csv") as csvfile:
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        aeg.append(i[0])

with open("SeccCensals_Municipis_totes.csv") as csvfile2:
    munireader = csv.reader(csvfile2, delimiter=";")
    next(munireader)
    for r in munireader:
        if r[1] not in municipisSC:
            municipisSC[r[1]] = [r[0]]
        else:
            municipisSC[r[1]].append(r[0])
        municipis.append(r[1])

#################################################### Definicio diccionaris #############################################
for m in municipis:
    resultatsMuni_immb[m] = {"Residencial": 0, "Comercial": 0, "Oficines": 0, "HotelerRestauracio": 0, "Public": 0,
                            "EnsenyamentCultural": 0, "Esportiu": 0, "Piscines": 0, "Industrial": 0,
                            "IndustrialResta": 0, "Aparcament": 0, "Altres": 0, "Total_immb_us_prn": 0, "IMMB_EXC_V": 0,
                            "IMMB_PRN_75_V": 0, "IMMB_PRN_50_V": 0, "IMMB_AMB_V": 0, "IMMB_NO_V": 0,
                            "Total_immb_tipus_resi": 0, "DivisioHor": 0, "NoDivisioHor": 0, "Total_immb_tipus_prop": 0,
                            "U": 0, "P_2a4": 0, "P_5a9": 0, "P_10a19": 0, "P_20a39": 0, "P_40mes": 0, "Total_num_v": 0,
                            "EAI": 0, "EAV": 0, "EVE": 0, "EFI": 0, "IND": 0, "Total_Ord": 0, "FS35": 0, "3660": 0,
                            "6180": 0, "8107": 0, "08EN": 0, "Total_anycons": 0, "De P-1 en avall": 0,
                            "De PB a PB+2": 0, "De PB+3 o mes": 0, "Total_immb_numplantes": 0}
    resultatsMuni_viv[m] = {"Residencial": 0, "Comercial": 0, "Oficines": 0, "HotelerRestauracio": 0, "Public": 0,
                            "EnsenyamentCultural": 0, "Esportiu": 0, "Piscines": 0, "Industrial": 0,
                            "IndustrialResta": 0, "Aparcament": 0, "Altres": 0, "Total_immb_us_prn": 0, "IMMB_EXC_V": 0,
                            "IMMB_PRN_75_V": 0, "IMMB_PRN_50_V": 0, "IMMB_AMB_V": 0, "IMMB_NO_V": 0,
                            "Total_immb_tipus_resi": 0, "DivisioHor": 0, "NoDivisioHor": 0, "Total_immb_tipus_prop": 0,
                            "U": 0, "P_2a4": 0, "P_5a9": 0, "P_10a19": 0, "P_20a39": 0, "P_40mes": 0, "Total_num_v": 0,
                            "EAI": 0, "EAV": 0, "EVE": 0, "EFI": 0, "IND": 0, "Total_Ord": 0, "FS35": 0, "3660": 0,
                            "6180": 0, "8107": 0, "08EN": 0, "Total_anycons": 0, "De P-1 en avall": 0,
                            "De PB a PB+2": 0, "De PB+3 o mes": 0, "Total_immb_numplantes": 0}
    resultatsMuni_SupSBR[m] = {"Residencial": 0, "Comercial": 0, "Oficines": 0, "HotelerRestauracio": 0, "Public": 0,
                            "EnsenyamentCultural": 0, "Esportiu": 0, "Piscines": 0, "Industrial": 0,
                            "IndustrialResta": 0, "Aparcament": 0, "Altres": 0, "Total_immb_us_prn": 0, "IMMB_EXC_V": 0,
                            "IMMB_PRN_75_V": 0, "IMMB_PRN_50_V": 0, "IMMB_AMB_V": 0, "IMMB_NO_V": 0,
                            "Total_immb_tipus_resi": 0, "DivisioHor": 0, "NoDivisioHor": 0, "Total_immb_tipus_prop": 0,
                            "U": 0, "P_2a4": 0, "P_5a9": 0, "P_10a19": 0, "P_20a39": 0, "P_40mes": 0, "Total_num_v": 0,
                            "EAI": 0, "EAV": 0, "EVE": 0, "EFI": 0, "IND": 0, "Total_Ord": 0, "FS35": 0, "3660": 0,
                            "6180": 0, "8107": 0, "08EN": 0, "Total_anycons": 0, "De P-1 en avall": 0,
                            "De PB a PB+2": 0, "De PB+3 o mes": 0, "Total_immb_numplantes": 0}
    resultatsMuni_SupVivSBR[m] = {"Residencial": 0, "Comercial": 0, "Oficines": 0, "HotelerRestauracio": 0, "Public": 0,
                            "EnsenyamentCultural": 0, "Esportiu": 0, "Piscines": 0, "Industrial": 0,
                            "IndustrialResta": 0, "Aparcament": 0, "Altres": 0, "Total_immb_us_prn": 0, "IMMB_EXC_V": 0,
                            "IMMB_PRN_75_V": 0, "IMMB_PRN_50_V": 0, "IMMB_AMB_V": 0, "IMMB_NO_V": 0,
                            "Total_immb_tipus_resi": 0, "DivisioHor": 0, "NoDivisioHor": 0, "Total_immb_tipus_prop": 0,
                            "U": 0, "P_2a4": 0, "P_5a9": 0, "P_10a19": 0, "P_20a39": 0, "P_40mes": 0, "Total_num_v": 0,
                            "EAI": 0, "EAV": 0, "EVE": 0, "EFI": 0, "IND": 0, "Total_Ord": 0, "FS35": 0, "3660": 0,
                            "6180": 0, "8107": 0, "08EN": 0, "Total_anycons": 0, "De P-1 en avall": 0,
                            "De PB a PB+2": 0, "De PB+3 o mes": 0, "Total_immb_numplantes": 0}

#################################################### Analisi variables #################################################
for r in conjuntRef:
    r = list(r)
    if r[1] in aeg:
        conjuntRefAEG += 1
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

#################################################### Immb_us_prn #######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[17] * 10 <= r[16] and ((r[3] != "IMMB_NO_V" and r[16] > 0) or
                                                                     r[3] == "IMMB_NO_V"):
        if r[2] == "AltresCal" or r[2] == "AltresNoCal" or r[2] == "Emmagatzematge" or r[2] == "Comu" or \
                r[2] == "VincViv":
            resultatsMuni_immb[r[19]]["Altres"] += 1
            resultatsMuni_viv[r[19]]["Altres"] += r[6]
            resultatsMuni_SupSBR[r[19]]["Altres"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["Altres"] += r[16]
        elif r[2] != "Comu" and r[2] != "VincViv":
            resultatsMuni_immb[r[19]][r[2]] += 1
            resultatsMuni_viv[r[19]][r[2]] += r[6]
            resultatsMuni_SupSBR[r[19]][r[2]] += r[15]
            resultatsMuni_SupVivSBR[r[19]][r[2]] += r[16]
        resultatsMuni_immb[r[19]]["Total_immb_us_prn"] += 1
        resultatsMuni_viv[r[19]]["Total_immb_us_prn"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_immb_us_prn"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_immb_us_prn"] += r[16]

#################################################### Immb_tipus_resi ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[17] * 10 <= r[16]:
        if r[3] == "IMMB_NO_V":
            resultatsMuni_immb[r[19]][r[3]] += 1
            resultatsMuni_viv[r[19]][r[3]] += r[6]
            resultatsMuni_SupSBR[r[19]][r[3]] += r[15]
            resultatsMuni_SupVivSBR[r[19]][r[3]] += r[16]
            resultatsMuni_immb[r[19]]["Total_immb_tipus_resi"] += 1
            resultatsMuni_viv[r[19]]["Total_immb_tipus_resi"] += r[6]
            resultatsMuni_SupSBR[r[19]]["Total_immb_tipus_resi"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["Total_immb_tipus_resi"] += r[16]
        elif r[16] > 0:
            resultatsMuni_immb[r[19]][r[3]] += 1
            resultatsMuni_viv[r[19]][r[3]] += r[6]
            resultatsMuni_SupSBR[r[19]][r[3]] += r[15]
            resultatsMuni_SupVivSBR[r[19]][r[3]] += r[16]
            resultatsMuni_immb[r[19]]["Total_immb_tipus_resi"] += 1
            resultatsMuni_viv[r[19]]["Total_immb_tipus_resi"] += r[6]
            resultatsMuni_SupSBR[r[19]]["Total_immb_tipus_resi"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["Total_immb_tipus_resi"] += r[16]

#################################################### Immb_tipus_prop ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        if r[5] == "U":
            resultatsMuni_immb[r[19]]["NoDivisioHor"] += 1
            resultatsMuni_viv[r[19]]["NoDivisioHor"] += r[6]
            resultatsMuni_SupSBR[r[19]]["NoDivisioHor"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["NoDivisioHor"] += r[16]
        elif r[5] == "P":
            resultatsMuni_immb[r[19]]["DivisioHor"] += 1
            resultatsMuni_viv[r[19]]["DivisioHor"] += r[6]
            resultatsMuni_SupSBR[r[19]]["DivisioHor"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["DivisioHor"] += r[16]
        resultatsMuni_immb[r[19]]["Total_immb_tipus_prop"] += 1
        resultatsMuni_viv[r[19]]["Total_immb_tipus_prop"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_immb_tipus_prop"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_immb_tipus_prop"] += r[16]

#################################################### Immb_num_v ########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni_immb[r[19]][r[7]] += 1
        resultatsMuni_viv[r[19]][r[7]] += r[6]
        resultatsMuni_SupSBR[r[19]][r[7]] += r[15]
        resultatsMuni_SupVivSBR[r[19]][r[7]] += r[16]
        resultatsMuni_immb[r[19]]["Total_num_v"] += 1
        resultatsMuni_viv[r[19]]["Total_num_v"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_num_v"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_num_v"] += r[16]

#################################################### Immb_ord ##########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        if r[8] == "IND" or r[8] == "NO_V":
            resultatsMuni_immb[r[19]]["IND"] += 1
            resultatsMuni_viv[r[19]]["IND"] += r[6]
            resultatsMuni_SupSBR[r[19]]["IND"] += r[15]
            resultatsMuni_SupVivSBR[r[19]]["IND"] += r[16]
        else:
            resultatsMuni_immb[r[19]][r[8]] += 1
            resultatsMuni_viv[r[19]][r[8]] += r[6]
            resultatsMuni_SupSBR[r[19]][r[8]] += r[15]
            resultatsMuni_SupVivSBR[r[19]][r[8]] += r[16]
        resultatsMuni_immb[r[19]]["Total_Ord"] += 1
        resultatsMuni_viv[r[19]]["Total_Ord"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_Ord"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_Ord"] += r[16]

#################################################### Immb_anycons ######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni_immb[r[19]][r[10]] += 1
        resultatsMuni_viv[r[19]][r[10]] += r[6]
        resultatsMuni_SupSBR[r[19]][r[10]] += r[15]
        resultatsMuni_SupVivSBR[r[19]][r[10]] += r[16]
        resultatsMuni_immb[r[19]]["Total_anycons"] += 1
        resultatsMuni_viv[r[19]]["Total_anycons"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_anycons"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_anycons"] += r[16]

#################################################### Immb_numplantes ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[16] > 0 and r[17] * 10 <= r[16]:
        resultatsMuni_immb[r[19]][r[12]] += 1
        resultatsMuni_viv[r[19]][r[12]] += r[6]
        resultatsMuni_SupSBR[r[19]][r[12]] += r[15]
        resultatsMuni_SupVivSBR[r[19]][r[12]] += r[16]
        resultatsMuni_immb[r[19]]["Total_immb_numplantes"] += 1
        resultatsMuni_viv[r[19]]["Total_immb_numplantes"] += r[6]
        resultatsMuni_SupSBR[r[19]]["Total_immb_numplantes"] += r[15]
        resultatsMuni_SupVivSBR[r[19]]["Total_immb_numplantes"] += r[16]

################################################ PRINT #################################################################
print("")
print("Total referències", end=": ")
print(len(conjuntRef))
print("Total referències àmbit", end=": ")
print(conjuntRefAEG)
print("")

variables = ["Residencial", "Comercial", "Oficines", "HotelerRestauracio", "Public", "EnsenyamentCultural",
             "Esportiu", "Piscines", "Industrial", "IndustrialResta", "Aparcament", "Altres", "Total_immb_us_prn", 
             "IMMB_EXC_V", "IMMB_PRN_75_V", "IMMB_PRN_50_V", "IMMB_AMB_V", "IMMB_NO_V", "Total_immb_tipus_resi", 
             "NoDivisioHor", "DivisioHor", "Total_immb_tipus_prop", "U", "P_2a4", "P_5a9", "P_10a19", "P_20a39",
             "P_40mes", "Total_num_v", "EAI", "EAV", "EVE", "EFI", "IND", "Total_Ord", "FS35", "3660", "6180", "8107", 
             "08EN", "Total_anycons", "De P-1 en avall", "De PB a PB+2", "De PB+3 o mes", "Total_immb_numplantes"]

################################################ Creació DataFrames ####################################################
resultatsMuni_immbDF = pd.DataFrame(resultatsMuni_immb)
resultatsMuni_immbDF = resultatsMuni_immbDF.reindex(variables)
resultatsMuni_vivDF = pd.DataFrame(resultatsMuni_viv)
resultatsMuni_vivDF = resultatsMuni_vivDF.reindex(variables)
resultatsMuni_SupSBRDF = pd.DataFrame(resultatsMuni_SupSBR)
resultatsMuni_SupSBRDF = resultatsMuni_SupSBRDF.reindex(variables)
resultatsMuni_SupVivSBRDF = pd.DataFrame(resultatsMuni_SupVivSBR)
resultatsMuni_SupVivSBRDF = resultatsMuni_SupVivSBRDF.reindex(variables)

################################################ Guardar a excel #######################################################
writer = ExcelWriter("ResultatsArqMuni.xlsx", engine="xlsxwriter")
resultatsMuni_immbDF.to_excel(writer, sheet_name="Immb")
resultatsMuni_vivDF.to_excel(writer, sheet_name="Viv")
resultatsMuni_SupSBRDF.to_excel(writer, sheet_name="SupSBR")
resultatsMuni_SupVivSBRDF.to_excel(writer, sheet_name="SupVivSBR")
writer.save()


