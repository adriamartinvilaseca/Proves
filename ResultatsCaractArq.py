import psycopg2
import csv
import copy
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

conjuntRA = set(getReferencies(cursor))

conn.commit()
cursor.close()
conn.close()


nomsVariables = ["immb", "viv", "m2_SBR", "m2_VIV_SBR"]
aeg = []
# ref_ord = []
ref_aeg = []

with open("SeccCensals_AEG.csv") as csvfile :
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        aeg.append(i[0])

# with open("RefNumv_antic.csv") as csvfile2 :
    # compreader = csv.reader(csvfile2, delimiter=";")
    # next(compreader)
    # for c in compreader:
        # ref_ord.append(c[0])

#################################################### Definicio diccionaris #############################################
immb_us_prn = {
    "Residencial": [0, 0, 0, 0],
    "Comercial": [0, 0, 0, 0],
    "Oficines": [0, 0, 0, 0],
    "HotelerRestauracio": [0, 0, 0, 0],
    "Public": [0, 0, 0, 0],
    "EnsenyamentCultural": [0, 0, 0, 0],
    "Esportiu": [0, 0, 0, 0],
    "Piscina": [0, 0, 0, 0],
    "Industrial": [0, 0, 0, 0],
    "IndustrialResta": [0, 0, 0, 0],
    "Aparcament": [0, 0, 0, 0],
    "Altres": [0, 0, 0, 0],
    "Error": [0, 0, 0, 0],
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
    "PSTR": [0, 0, 0, 0],
    "PBaPB2": [0, 0, 0, 0],
    "PB3omes": [0, 0, 0, 0],
    "IND": [0, 0, 0, 0],
    "Total": [0, 0, 0, 0]
}

ordenacio = []
numviviendes = []
anyconstruccio = []
plantaltura = []
propietat = []

for r in conjuntRA:
    r = list(r)
    if r[4] is not None:
        r[4] = float(r[4])
    if r[6] is not None:
        r[6] = int(r[6])
    # if r[9] is not None:
        # r[9] = float(r[9])
    # if r[11] is not None:
        # r[11] = int(r[11])
    if r[15] is not None:
        r[15] = float(r[15])
    if r[16] is not None:
        r[16] = float(r[16])
    # print(r, end=", ")
    # print(" ")
    # if r[1] in aeg and r[6] is not None and r[6] != 0:
        # ref_aeg.append(r[0])

#################################################### Immb_us_prn #######################################################
    if r[1] in aeg and r[5] != "P_CORR":
        if r[2] == "Residencial":
            immb_us_prn["Residencial"][0] += 1
            immb_us_prn["Residencial"][1] += r[6]
            immb_us_prn["Residencial"][2] += r[15]
            immb_us_prn["Residencial"][3] += r[16]
        elif r[2] == "Comercial":
            immb_us_prn["Comercial"][0] += 1
            immb_us_prn["Comercial"][1] += r[6]
            immb_us_prn["Comercial"][2] += r[15]
            immb_us_prn["Comercial"][3] += r[16]
        elif r[2] == "Oficines":
            immb_us_prn["Oficines"][0] += 1
            immb_us_prn["Oficines"][1] += r[6]
            immb_us_prn["Oficines"][2] += r[15]
            immb_us_prn["Oficines"][3] += r[16]
        elif r[2] == "HotelerRestauracio":
            immb_us_prn["HotelerRestauracio"][0] += 1
            immb_us_prn["HotelerRestauracio"][1] += r[6]
            immb_us_prn["HotelerRestauracio"][2] += r[15]
            immb_us_prn["HotelerRestauracio"][3] += r[16]
        elif r[2] == "Public":
            immb_us_prn["Public"][0] += 1
            immb_us_prn["Public"][1] += r[6]
            immb_us_prn["Public"][2] += r[15]
            immb_us_prn["Public"][3] += r[16]
        elif r[2] == "EnsenyamentCultural":
            immb_us_prn["EnsenyamentCultural"][0] += 1
            immb_us_prn["EnsenyamentCultural"][1] += r[6]
            immb_us_prn["EnsenyamentCultural"][2] += r[15]
            immb_us_prn["EnsenyamentCultural"][3] += r[16]
        elif r[2] == "Esportiu":
            immb_us_prn["Esportiu"][0] += 1
            immb_us_prn["Esportiu"][1] += r[6]
            immb_us_prn["Esportiu"][2] += r[15]
            immb_us_prn["Esportiu"][3] += r[16]
        elif r[2] == "Piscines":
            immb_us_prn["Piscina"][0] += 1
            immb_us_prn["Piscina"][1] += r[6]
            immb_us_prn["Piscina"][2] += r[15]
            immb_us_prn["Piscina"][3] += r[16]
        elif r[2] == "Industrial":
            immb_us_prn["Industrial"][0] += 1
            immb_us_prn["Industrial"][1] += r[6]
            immb_us_prn["Industrial"][2] += r[15]
            immb_us_prn["Industrial"][3] += r[16]
        elif r[2] == "IndustrialResta":
            immb_us_prn["IndustrialResta"][0] += 1
            immb_us_prn["IndustrialResta"][1] += r[6]
            immb_us_prn["IndustrialResta"][2] += r[15]
            immb_us_prn["IndustrialResta"][3] += r[16]
        elif r[2] == "Aparcament":
            immb_us_prn["Aparcament"][0] += 1
            immb_us_prn["Aparcament"][1] += r[6]
            immb_us_prn["Aparcament"][2] += r[15]
            immb_us_prn["Aparcament"][3] += r[16]
        elif r[2] == "AltresCal" or r[2] == "AltresNoCal" or r[2] == "Emmagatzematge":
            immb_us_prn["Altres"][0] += 1
            immb_us_prn["Altres"][1] += r[6]
            immb_us_prn["Altres"][2] += r[15]
            immb_us_prn["Altres"][3] += r[16]
        else :
            immb_us_prn["Error"][0] += 1
            immb_us_prn["Error"][1] += r[6]
            immb_us_prn["Error"][2] += r[15]
            immb_us_prn["Error"][3] += r[16]
        immb_us_prn["Total"][0] = immb_us_prn["Residencial"][0] + immb_us_prn["Comercial"][0] + \
                                  immb_us_prn["Oficines"][0] + immb_us_prn["HotelerRestauracio"][0] + \
                                  immb_us_prn["Public"][0] + immb_us_prn["EnsenyamentCultural"][0] + \
                                  immb_us_prn["Esportiu"][0] + immb_us_prn["Piscina"][0] + \
                                  immb_us_prn["Industrial"][0] + immb_us_prn["IndustrialResta"][0] + \
                                  immb_us_prn["Aparcament"][0] + immb_us_prn["Altres"][0] + immb_us_prn["Error"][0]
        immb_us_prn["Total"][1] = immb_us_prn["Residencial"][1] + immb_us_prn["Comercial"][1] + \
                                  immb_us_prn["Oficines"][1] + immb_us_prn["HotelerRestauracio"][1] + \
                                  immb_us_prn["Public"][1] + immb_us_prn["EnsenyamentCultural"][1] + \
                                  immb_us_prn["Esportiu"][1] + immb_us_prn["Piscina"][1] + \
                                  immb_us_prn["Industrial"][1] + immb_us_prn["IndustrialResta"][1] + \
                                  immb_us_prn["Aparcament"][1] + immb_us_prn["Altres"][1] + immb_us_prn["Error"][1]
        immb_us_prn["Total"][2] = immb_us_prn["Residencial"][2] + immb_us_prn["Comercial"][2] + \
                                  immb_us_prn["Oficines"][2] + immb_us_prn["HotelerRestauracio"][2] + \
                                  immb_us_prn["Public"][2] + immb_us_prn["EnsenyamentCultural"][2] + \
                                  immb_us_prn["Esportiu"][2] + immb_us_prn["Piscina"][2] + \
                                  immb_us_prn["Industrial"][2] + immb_us_prn["IndustrialResta"][2] + \
                                  immb_us_prn["Aparcament"][2] + immb_us_prn["Altres"][2] + immb_us_prn["Error"][2]
        immb_us_prn["Total"][3] = immb_us_prn["Residencial"][3] + immb_us_prn["Comercial"][3] + \
                                  immb_us_prn["Oficines"][3] + immb_us_prn["HotelerRestauracio"][3] + \
                                  immb_us_prn["Public"][3] + immb_us_prn["EnsenyamentCultural"][3] + \
                                  immb_us_prn["Esportiu"][3] + immb_us_prn["Piscina"][3] + \
                                  immb_us_prn["Industrial"][3] + immb_us_prn["IndustrialResta"][3] + \
                                  immb_us_prn["Aparcament"][3] + immb_us_prn["Altres"][3] + immb_us_prn["Error"][3]

#################################################### Immb_tipus_resi ###################################################
    if r[1] in aeg and r[5] != "P_CORR":
        if r[3] == "IMMB_EXC_V":
            immb_tipus_resi["IMMB_EXC_V"][0] += 1
            immb_tipus_resi["IMMB_EXC_V"][1] += r[6]
            immb_tipus_resi["IMMB_EXC_V"][2] += r[15]
            immb_tipus_resi["IMMB_EXC_V"][3] += r[16]
        elif r[3] == "IMMB_PRN_75_V":
            immb_tipus_resi["IMMB_PRN_75_V"][0] += 1
            immb_tipus_resi["IMMB_PRN_75_V"][1] += r[6]
            immb_tipus_resi["IMMB_PRN_75_V"][2] += r[15]
            immb_tipus_resi["IMMB_PRN_75_V"][3] += r[16]
        elif r[3] == "IMMB_PRN_50_V":
            immb_tipus_resi["IMMB_PRN_50_V"][0] += 1
            immb_tipus_resi["IMMB_PRN_50_V"][1] += r[6]
            immb_tipus_resi["IMMB_PRN_50_V"][2] += r[15]
            immb_tipus_resi["IMMB_PRN_50_V"][3] += r[16]
        elif r[3] == "IMMB_AMB_V":
            immb_tipus_resi["IMMB_AMB_V"][0] += 1
            immb_tipus_resi["IMMB_AMB_V"][1] += r[6]
            immb_tipus_resi["IMMB_AMB_V"][2] += r[15]
            immb_tipus_resi["IMMB_AMB_V"][3] += r[16]
        elif r[3] == "IMMB_NO_V":
            immb_tipus_resi["IMMB_NO_V"][0] += 1
            immb_tipus_resi["IMMB_NO_V"][1] += r[6]
            immb_tipus_resi["IMMB_NO_V"][2] += r[15]
            immb_tipus_resi["IMMB_NO_V"][3] += r[16]
        immb_tipus_resi["Total"][0] += 1
        immb_tipus_resi["Total"][1] += r[6]
        immb_tipus_resi["Total"][2] += r[15]
        immb_tipus_resi["Total"][3] += r[16]

#################################################### Immb_tipus_prop ###################################################
    if r[1] in aeg and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[5] == "U" or r[5] == "P_CORR":
            immb_tipus_prop["DivisioHor"][0] += 1
            immb_tipus_prop["DivisioHor"][1] += r[6]
            immb_tipus_prop["DivisioHor"][2] += r[15]
            immb_tipus_prop["DivisioHor"][3] += r[16]
        elif r[5] == "P":
            immb_tipus_prop["NoDivisioHor"][0] += 1
            immb_tipus_prop["NoDivisioHor"][1] += r[6]
            immb_tipus_prop["NoDivisioHor"][2] += r[15]
            immb_tipus_prop["NoDivisioHor"][3] += r[16]
        else:
            if r[5] not in propietat:
                propietat.append(r[5])
        immb_tipus_prop["Total"][0] += 1
        immb_tipus_prop["Total"][1] += r[6]
        immb_tipus_prop["Total"][2] += r[15]
        immb_tipus_prop["Total"][3] += r[16]

#################################################### Immb_num_v ########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[7] == "U":
            immb_num_v["U"][0] += 1
            immb_num_v["U"][1] += r[6]
            immb_num_v["U"][2] += r[15]
            immb_num_v["U"][3] += r[16]
        elif r[7] == "P_2a4":
            immb_num_v["P_2a4"][0] += 1
            immb_num_v["P_2a4"][1] += r[6]
            immb_num_v["P_2a4"][2] += r[15]
            immb_num_v["P_2a4"][3] += r[16]
        elif r[7] == "P_5a9":
            immb_num_v["P_5a9"][0] += 1
            immb_num_v["P_5a9"][1] += r[6]
            immb_num_v["P_5a9"][2] += r[15]
            immb_num_v["P_5a9"][3] += r[16]
        elif r[7] == "P_10a19":
            immb_num_v["P_10a19"][0] += 1
            immb_num_v["P_10a19"][1] += r[6]
            immb_num_v["P_10a19"][2] += r[15]
            immb_num_v["P_10a19"][3] += r[16]
        elif r[7] == "P_20a39":
            immb_num_v["P_20a39"][0] += 1
            immb_num_v["P_20a39"][1] += r[6]
            immb_num_v["P_20a39"][2] += r[15]
            immb_num_v["P_20a39"][3] += r[16]
        elif r[7] == "P_40mes":
            immb_num_v["P_40mes"][0] += 1
            immb_num_v["P_40mes"][1] += r[6]
            immb_num_v["P_40mes"][2] += r[15]
            immb_num_v["P_40mes"][3] += r[16]
        else:
            if r[5] not in numviviendes:
                numviviendes.append(r[5])
        immb_num_v["Total"][0] += 1
        immb_num_v["Total"][1] += r[6]
        immb_num_v["Total"][2] += r[15]
        immb_num_v["Total"][3] += r[16]

#################################################### Immb_ord ##########################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[8] == "EAI":
            immb_ord["EAI"][0] += 1
            immb_ord["EAI"][1] += r[6]
            immb_ord["EAI"][2] += r[15]
            immb_ord["EAI"][3] += r[16]
        elif r[8] == "EAV":
            immb_ord["EAV"][0] += 1
            immb_ord["EAV"][1] += r[6]
            immb_ord["EAV"][2] += r[15]
            immb_ord["EAV"][3] += r[16]
        elif r[8] == "EVE":
            immb_ord["EVE"][0] += 1
            immb_ord["EVE"][1] += r[6]
            immb_ord["EVE"][2] += r[15]
            immb_ord["EVE"][3] += r[16]
        elif r[8] == "EFI":
            immb_ord["EFI"][0] += 1
            immb_ord["EFI"][1] += r[6]
            immb_ord["EFI"][2] += r[15]
            immb_ord["EFI"][3] += r[16]
        elif r[8] == "IND" or r[8] == "NO_V":
            immb_ord["IND"][0] += 1
            immb_ord["IND"][1] += r[6]
            immb_ord["IND"][2] += r[15]
            immb_ord["IND"][3] += r[16]
        else:
            if r[5] not in ordenacio:
                ordenacio.append(r[5])
        immb_ord["Total"][0] += 1
        immb_ord["Total"][1] += r[6]
        immb_ord["Total"][2] += r[15]
        immb_ord["Total"][3] += r[16]

#################################################### Immb_anycons ######################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[10] == "FS35":
            immb_anycons["FS35"][0] += 1
            immb_anycons["FS35"][1] += r[6]
            immb_anycons["FS35"][2] += r[15]
            immb_anycons["FS35"][3] += r[16]
        elif r[10] == "3660":
            immb_anycons["3660"][0] += 1
            immb_anycons["3660"][1] += r[6]
            immb_anycons["3660"][2] += r[15]
            immb_anycons["3660"][3] += r[16]
        elif r[10] == "6180":
            immb_anycons["6180"][0] += 1
            immb_anycons["6180"][1] += r[6]
            immb_anycons["6180"][2] += r[15]
            immb_anycons["6180"][3] += r[16]
        elif r[10] == "8107":
            immb_anycons["8107"][0] += 1
            immb_anycons["8107"][1] += r[6]
            immb_anycons["8107"][2] += r[15]
            immb_anycons["8107"][3] += r[16]
        elif r[10] == "08EN":
            immb_anycons["08EN"][0] += 1
            immb_anycons["08EN"][1] += r[6]
            immb_anycons["08EN"][2] += r[15]
            immb_anycons["08EN"][3] += r[16]
        else:
            if r[5] not in anyconstruccio:
                anyconstruccio.append(r[5])
        immb_anycons["Total"][0] += 1
        immb_anycons["Total"][1] += r[6]
        immb_anycons["Total"][2] += r[15]
        immb_anycons["Total"][3] += r[16]

#################################################### Immb_numplantes ###################################################
    if r[1] in aeg and r[5] != "P_CORR" and r[6] is not None and r[6] != 0 and r[12] != "":
        if r[12] == "De P-1 en avall":
            immb_numplantes["PSTR"][0] += 1
            immb_numplantes["PSTR"][1] += r[6]
            immb_numplantes["PSTR"][2] += r[15]
            immb_numplantes["PSTR"][3] += r[16]
        elif r[12] == "De PB a PB+2":
            immb_numplantes["PBaPB2"][0] += 1
            immb_numplantes["PBaPB2"][1] += r[6]
            immb_numplantes["PBaPB2"][2] += r[15]
            immb_numplantes["PBaPB2"][3] += r[16]
        elif r[12] == "De PB+3 o mes":
            immb_numplantes["PB3omes"][0] += 1
            immb_numplantes["PB3omes"][1] += r[6]
            immb_numplantes["PB3omes"][2] += r[15]
            immb_numplantes["PB3omes"][3] += r[16]
        else:
            if r[5] not in plantaltura:
                plantaltura.append(r[5])
        immb_numplantes["Total"][0] += 1
        immb_numplantes["Total"][1] += r[6]
        immb_numplantes["Total"][2] += r[15]
        immb_numplantes["Total"][3] += r[16]

################################################ PRINT #################################################################
print("Total referències: ", end=": ")
print(len(conjuntRA))
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

# for a in ref_aeg:
    # if a not in ref_ord:
        # print(a)
# print(len(ref_aeg))
# print(len(ref_ord))

# print(nopresent)

# print(ref_aeg)

# num_v_dict = {}

# for c in conjuntRA:
#    if c[1] in aeg:
#        num_v_dict[c[0]] = c[6]

# num_v_dictDF = pd.DataFrame(num_v_dict, index=[0])
# num_v_dictDF = num_v_dictDF.T

# num_v_dictDF.to_csv("RefNumv.csv")

print(immb_us_prn)

print(ordenacio)
print(numviviendes)
print(anyconstruccio)
print(plantaltura)
print(propietat)
