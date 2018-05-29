import psycopg2
import csv
import copy
from pandas import DataFrame


def getReferencies(cursor):
    cursor.execute("SELECT REFCAT, SECC_CENSAL, IMMB_US_PRN, IMMB_TIPUS, IMMB_TIPUS_PERCENT, UNI_PLURI_CORR, NUM_V, "
                   "PLURI_NUM_V, ORD, ANYCONST_SUP_V, ANYCONST_ETAPA_SUP_V, AL_V_MAX, AL_IMMB, SEGMENT_100, SEGMENT_10,"
                   "SUP_SBR, SUP_VIV_SBR FROM referencies_alpha;")
    return cursor.fetchall()

def getRef(cursor, refcat):
    cursor.execute("SELECT * FROM referencies_alpha WHERE REFCAT = '{}';".format(refcat))
    return cursor.fetchall()

def getImmb_us_prn(cursor):
    cursor.execute("SELECT COUNT(id) AS Immb, SUM(num_v::integer) AS Viv, SUM(sup_sbr::integer) AS M2SBR, "
                   "SUM(sup_viv_sbr::integer) AS M2VIVSBR FROM referencies_alpha WHERE secc_censal GROUP BY immb_us_prn "
                   "ORDER BY immb_us_prn")
    return cursor.fetchall()


conn_string = "host='bbddalphanumeric.czciosgdrat6.eu-west-1.rds.amazonaws.com' dbname='DBalphanumeric' user='paulcharbonneau' password='db_testing_alpha'"
print("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

conjuntRA = set(getReferencies(cursor))

conn.commit()
cursor.close()
conn.close()


nomsVariables = ["immb", "viv", "m2sbr", "m2vivsbr"]
variables = [0,0,0,0]
aeg = []

with open("SeccCensals_AEG.csv") as csvfile :
    ambitreader = csv.reader(csvfile, delimiter=",")
    next(ambitreader)
    for i in ambitreader:
        aeg.append(i[0])

immb_us_prn = {
    "Residencial": variables,
    "Comercial": variables,
    "Oficines": variables,
    "HotelerRestauracio": variables,
    "Public": variables,
    "EnsenyamentCultural": variables,
    "Esportiu": variables,
    "Piscina": variables,
    "Industrial": variables,
    "IndustrialResta": variables,
    "Aparcament": variables,
}
immb_tipus_resi = {
    "EXC_100": variables,
    "ESS_75100": variables,
    "PRI_5075": variables,
    "PAR_0050": variables,
    "NO_RES": variables,
}
immb_tipus_prop = {
    "DivisioHor": variables,
    "NoDivisioHor": variables,
}
immb_num_v = {
    "U": variables,
    "PLURI_2A4": variables,
    "PRI_5075": variables,
    "PAR_0050": variables,
    "NO_RES": variables,
}
immb_ord = {
    "EAI": variables,
    "EAV": variables,
    "EVE": variables,
    "EFI": variables,
    "IND": variables,
}
immb_anycons = {
    "FS35": variables,
    "3660": variables,
    "6180": variables,
    "8107": variables,
    "08EN": variables,
}
immb_numplantes = {
    "PSTR": variables,
    "PBaPB2": variables,
    "PB3omes": variables,
}


for r in conjuntRA:
    print(r, end=", ")
    if r[4] is not None:
        float(r[4])
    if r[6] is not None:
        int(r[6])
    if r[9] is not None:
        int(r[9])
    if r[11] is not None:
        int(r[11])
    if r[15] is not None:
        float(r[15])
    if r[16] is not None:
        float(r[16])

    if r[6] != "P_CORR" and r[1] in aeg and r[7] is not None:

        # Immb_us_prn
        if r[3] == "Residencial":
            immb_us_prn["Residencial"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Comercial":
            immb_us_prn["Comercial"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Oficines":
            immb_us_prn["Oficines"] += [1, r[7], r[17], r[20]]
        elif r[3] == "HotelerRestauracio":
            immb_us_prn["HotelerRestauracio"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Public":
            immb_us_prn["Public"] += [1, r[7], r[17], r[20]]
        elif r[3] == "EnsenyamentCultural":
            immb_us_prn["EnsenyamentCultural"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Esportiu":
            immb_us_prn["Esportiu"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Piscina":
            immb_us_prn["Piscina"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Industrial":
            immb_us_prn["Industrial"] += [1, r[7], r[17], r[20]]
        elif r[3] == "IndustrialResta":
            immb_us_prn["IndustrialResta"] += [1, r[7], r[17], r[20]]
        elif r[3] == "Aparcament":
            immb_us_prn["Aparcament"] += [1, r[7], r[17], r[20]]

print(immb_us_prn)


