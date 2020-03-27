#!/usr/bin/python
import calendar
import mysql.connector
from mysql.connector import Error
import argparse
import datetime
import json
import getopt, sys


# -----------------------------------------------------------------------------------------------------------------
# add_cpu.py : extract CPU & wall times from the condorjobs MySQL database, and add them to the
#              UB Schedule MySQL database, in preparation for creating the UB schedule
#              spreadsheet.
# -----------------------------------------------------------------------------------------------------------------


def getResultFromQuery(query, which=5):
    cursor = db1Connection.cursor()
    cursor.execute(query)
    res = cursor.fetchone()
    # These weird if elses are to stop errors when there are no records returned at both the fetchone and [0] stages
    if res:
        record = res[0]
        if record:
            record = record
        else:
            record = 0
    else:
        record = 0
    cursor.close()

    if units == "hepspec06":
        c_units = 4.0
    else:
        c_units = 1.0

    if which == 0 or which == 5: c_units = 1.0
    return record * c_units


parser = argparse.ArgumentParser(description="Calculating nfs and castor disk usage")

year = datetime.date.today().year
month = datetime.date.today().month - 1
prevYear = year
prevMonth = month - 1

if month == 0:
    month = 12
    year = year - 1
    prevYear = year
    prevMonth = 11

parser.add_argument("-y", default=year, type=int, help="This is the year you want to anlayse the data in")
parser.add_argument("-m", default=month, type=int, help="This is the month you want to anlayse the data in")
parser.add_argument("-u", required=True, choices=["ksi2k", "hepspec06"], help="Units - either 'ksi2k' or 'hepspec06'")

args = parser.parse_args()

y = args.y
m = args.m
units = args.u

if y < 2000:
    print
    "ERROR: Invalid year specified"
    sys.exit()
else:
    year = y
    prevYear = year
if m < 1 or m > 12:
    print
    "ERROR: Invalid month specified."
    sys.exit()
else:
    monthNum = m
    prevMonth = monthNum - 1
    monthPrint = calendar.month_name[monthNum]
    if month == 0:
        month = 12
        year = year - 1
        prevMonth = 11
        prevYear = year

if prevMonth == 0:
    prevMonth = 12
    prevYear -= 1

prevMonthName = calendar.month_name[prevMonth]

print("NOTE: Working on year = " + str(year) + "; month = " + str(monthPrint))

d_units = ""

if units == "hepspec06":
    d_units = "HEP-SPEC06"
else:
    d_units = "KSI2K"

# Read from config file
with open('cps-db.json') as config_file:
    config = json.load(config_file)

server = config['host']
db = config['database']
user = config['user']
password = config['passwd']

try:
    db1Connection = mysql.connector.connect(host=server, database=db, user=user, password=password)
except Error as e:
    print("Error while connecting to MySQL for db1", e)

vo_list = ["ALICE", "ATLAS", "CMS", "LHCb", "Bio", "Dteam", "enmr", "ILC", "LIGO", "LSST", "MICE", "Pheno", "T2K",
           "SNO", "NA48/NA62", "UKQCD", "Others"]

vo_id = {}
my_not = []
i = 0
for vo in vo_list:
    query = "SELECT id FROM cps.vo_list WHERE name = '" + vo + "'"
    vo_id[i] = getResultFromQuery(query)
    if vo + str(i) != "Others":
        my_not.append(vo + str(i))
    i += 1
cpu = {}
wall = {}
eff = {}
waste = {}

# get CPU utilised, walltime utilised
i = 0
for vo in vo_list:
    if (vo == "Others"):
        query1 = "SELECT SUM(cpu_grid+cpu_nongrid) FROM resources_usage_per_vo WHERE year=" + str(
            year) + " AND month=" + str(month) + " AND vo NOT IN " + str(tuple(my_not))
        query2 = "SELECT SUM(walltime_grid+walltime_nongrid) FROM resources_usage_per_vo WHERE year=" + str(
            year) + " AND month=" + str(month) + " AND vo NOT IN " + str(tuple(my_not))
    else:
        query1 = "SELECT cpu_grid+cpu_nongrid FROM resources_usage_per_vo WHERE year=" + str(
            year) + " AND month=" + str(month) + " AND vo='" + str(vo_id[i]) + "'"
        query2 = "SELECT walltime_grid+walltime_nongrid FROM resources_usage_per_vo WHERE year=" + str(
            year) + " AND month=" + str(month) + " AND vo='" + str(vo_id[i]) + "'"

    cpu[vo_list[i]] = getResultFromQuery(query1, 1)
    wall[vo_list[i]] = getResultFromQuery(query2, 1)
    if cpu[vo_list[i]] < 0.0:  cpu[vo_list[i]] = 0
    if wall[vo_list[i]] < 0.0: wall[vo_list[i]] = 0

    # VO efficienceies
    if (wall[vo_list[i]] > 0.0):
        eff[vo_list[i]] = cpu[vo_list[i]] * 100.0 / wall[vo_list[i]]
        waste[vo_list[i]] = wall[vo_list[i]] - cpu[vo_list[i]]
        if waste[vo_list[i]] < 0:
            waste[vo_list[i]] = 0
    else:
        eff[vo_list[i]] = 0
        waste[vo_list[i]] = 0

    i += 1

# Totals (current month)
query = "SELECT SUM(cpu_grid+cpu_nongrid) FROM resources_usage_per_vo WHERE year=" + str(year) + " AND month=" + str(
    month)
cpu["total"] = getResultFromQuery(query, 1)

query = "SELECT SUM(walltime_grid+walltime_nongrid) FROM resources_usage_per_vo WHERE year=" + str(
    year) + " AND month=" + str(month)
wall["total"] = getResultFromQuery(query, 1)

# Totals (previous month)
query = "SELECT SUM(cpu_grid+cpu_nongrid) FROM resources_usage_per_vo WHERE year=" + str(
    prevYear) + " AND month=" + str(prevMonth)
cpu["total_prev"] = getResultFromQuery(query, 1)

query = "SELECT SUM(walltime_grid+walltime_nongrid) FROM resources_usage_per_vo WHERE year=" + str(
    prevYear) + " AND month=" + str(prevMonth)
wall["total_prev"] = getResultFromQuery(query, 1)

# Calculate cpu time, wall time & efficiencies LHC totals
cpu["lhc_total"] = cpu["ALICE"] + cpu["ATLAS"] + cpu["CMS"] + cpu["LHCb"]
wall["lhc_total"] = wall["ALICE"] + wall["ATLAS"] + wall["CMS"] + wall["LHCb"]

# Get total available wall time
query = "SELECT cpu FROM resources_deployed_total WHERE year=" + str(year) + " AND month=" + str(month)
total_dep_cpu = getResultFromQuery(query, 1)

if (wall["lhc_total"] > 0):
    eff["lhc_total"] = cpu["lhc_total"] * 100.0 / wall["lhc_total"]
    waste["lhc_total"] = wall["lhc_total"] - cpu["lhc_total"]
else:
    eff["lhc_total"] = ""
    waste["lhc_total"] = ""

# Calculate total efficiency
if (wall["total"] > 0):
    eff["total"] = round(cpu["total"] / wall["total"] * 100.0, 1)
    waste["total"] = round(wall["total"] - cpu["total"], 1)
else:
    eff["total"] = ""
    waste["total"] = ""
if (wall["total_prev"] > 0):
    eff["total_prev"] = round(cpu["total_prev"] / wall["total_prev"] * 100.0, 1)
    waste["total_prev"] = round(wall["total_prev"] - cpu["total_prev"], 1)
else:
    eff["total_prev"] = ""
    waste["total_prev"] = ""

# -----------------------------------------------------------------------------------------------------------------
# Write text for CPU efficiencies email
# -----------------------------------------------------------------------------------------------------------------

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
          "December"]
occupancy = round(wall["total"] / total_dep_cpu * 100, 1)
total_dep_cpu = round(total_dep_cpu, 0)

change_word = ""
if eff["total"] > eff["total_prev"]:
    change_word = "up"
elif eff["total"] < eff["total_prev"]:
    change_word = "down"
else:
    change_word = "unchanged"

wall_used_fmt = wall["total"]

print("\n <------ CPU efficiencies email ------>")
print("\n To: GRIDPP-UB\@JISCMAIL.AC.UK")
print("\n Subject: CPU Efficiencies at RAL ({0} {1}).".format(monthPrint, str(year)))

print("\n Here is some information about CPU use at the RAL Tier-1/A batch farm")
print("for {0} {1}.".format(monthPrint, year))

print("\n Global CPU efficiency (CPU time / wall time) was {0} in {1} at {2}, compared with {3} in {4}.".format(
    change_word,
    monthPrint,
    str(eff[
            "total"]),
    str(eff[
            "total_prev"]),
    prevMonthName))
print("Of {0} {1} months available wall time, {2} {3} months were used ({4} occupancy).".format(str(total_dep_cpu),
                                                                                                str(d_units),
                                                                                                str(wall_used_fmt),
                                                                                                str(d_units),
                                                                                                str(occupancy)))
print("\n Experiment summary:")
print("\n    Experiment       CPU Time      Wall Time         Wait    % Efficiency\n")
if (units == "hepspec06"):
    print("                               HEP-SPEC06 Months\n")
else:
    print("                           KSI2K Months\n")
print("         ALICE      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["ALICE"], wall["ALICE"], waste["ALICE"], eff["ALICE"]))
print("         ATLAS      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["ATLAS"], wall["ATLAS"], waste["ATLAS"], eff["ATLAS"]))
print(
    "           CMS      %9.2f      %9.2f     %9.2f     %9.2f \n" % (cpu["CMS"], wall["CMS"], waste["CMS"], eff["CMS"]))
print("          LHCb      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["LHCb"], wall["LHCb"], waste["LHCb"], eff["LHCb"]))
print("\n")
print("     LHC Total      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["lhc_total"], wall["lhc_total"], waste["lhc_total"], eff["lhc_total"]))
print("\n")
print("          ENMR      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["enmr"], wall["enmr"], waste["enmr"], eff["enmr"]))
print(
    "           ILC      %9.2f      %9.2f     %9.2f     %9.2f \n" % (cpu["ILC"], wall["ILC"], waste["ILC"], eff["ILC"]))
print("          LIGO      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["LIGO"], wall["LIGO"], waste["LIGO"], eff["LIGO"]))
print("          LSST      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["LSST"], wall["LSST"], waste["LSST"], eff["LSST"]))
print("          MICE      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["MICE"], wall["MICE"], waste["MICE"], eff["MICE"]))
print("     NA48/NA62      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["NA48/NA62"], wall["NA48/NA62"], waste["NA48/NA62"], eff["NA48/NA62"]))
print("         Pheno      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["Pheno"], wall["Pheno"], waste["Pheno"], eff["Pheno"]))
print(
    "          SNO       %9.2f      %9.2f     %9.2f     %9.2f \n" % (cpu["SNO"], wall["SNO"], waste["SNO"], eff["SNO"]))
print(
    "           T2K      %9.2f      %9.2f     %9.2f     %9.2f \n" % (cpu["T2K"], wall["T2K"], waste["T2K"], eff["T2K"]))
print("         UKQCD      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["UKQCD"], wall["UKQCD"], waste["UKQCD"], eff["UKQCD"]))
print("     DTeam/Ops      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["Dteam"], wall["Dteam"], waste["Dteam"], eff["Dteam"]))
print("        Others      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["Others"], wall["Others"], waste["Others"], eff["Others"]))
print("\n")
print("         Total      %9.2f      %9.2f     %9.2f     %9.2f \n" % (
    cpu["total"], wall["total"], waste["total"], eff["total"]))
print("References:")

print("\n    Tier-1 Schedule:")

print("\n        http://www.gridpp.rl.ac.uk/schedule/schedule.xls")
print("\n")
print("<------ end text for efficiencies email ------>\n")

# -----------------------------------------------------------------------------------------------------------------
# Get all required data from MySQL for disk deployment email
# -----------------------------------------------------------------------------------------------------------------

alloc = {}
depl = {}
i = 0
# Allocated disk for each VO
for vo in vo_list:
    query = "SELECT disk_alloc FROM resources_alloc_per_vo WHERE vo=" + str(vo_id[i]) + " AND year=" + str(
        year) + " AND month=" + str(month)
    alloc[vo_list[i]] = getResultFromQuery(query, 0)

    query = "SELECT disk_nfs+disk_castor_disk0+disk_castor_disk1 FROM resources_deployed_per_vo WHERE vo=" + str(
        vo_id[i]) + " AND year=" + str(year) + " AND month=" + str(month)
    depl[vo_list[i]] = getResultFromQuery(query, 0)

    i += 1

# ATLAS & LHCb cache
query = "SELECT disk_castor_disk0 FROM resources_deployed_per_vo WHERE vo=2 AND year=" + str(
    year) + " AND month=" + str(month)
depl["ATLAS_disk0"] = getResultFromQuery(query, 0)

query = "SELECT disk_castor_disk0 FROM resources_deployed_per_vo WHERE vo=4 AND year=" + str(
    year) + " AND month=" + str(month)
depl["LHCb_disk0"] = getResultFromQuery(query, 0)

# LHC total allocated
alloc["lhc_total"] = alloc["ATLAS"] + alloc["ALICE"] + alloc["CMS"] + alloc["LHCb"]

# LHC total (cache)
depl["lhc_total_disk0"] = depl["ATLAS_disk0"] + depl["LHCb_disk0"]

# Echo
query = "SELECT disk_echo FROM resources_deployed_per_vo WHERE vo=2 AND year=" + str(year) + " AND month=" + str(month)
depl["ATLAS_echo"] = getResultFromQuery(query, 0)
query = "SELECT disk_echo FROM resources_deployed_per_vo WHERE vo=3 AND year=" + str(year) + " AND month=" + str(month)
depl["CMS_echo"] = getResultFromQuery(query, 0)
query = "SELECT disk_echo FROM resources_deployed_per_vo WHERE vo=4 AND year=" + str(year) + " AND month=" + str(month)
depl["LHCb_echo"] = getResultFromQuery(query, 0)
depl["lhc_total_echo"] = depl["ATLAS_echo"] + depl["CMS_echo"] + depl["LHCb_echo"]
depl["total_echo"] = depl["ATLAS_echo"] + depl["CMS_echo"] + depl["LHCb_echo"]

# Full totals
query = "SELECT SUM(disk_alloc) FROM resources_alloc_per_vo WHERE year=" + str(year) + " AND month=" + str(month)
alloc["total"] = getResultFromQuery(query, 0)

# -----------------------------------------------------------------------------------------------------------------
# Write text for disk deployment email
# -----------------------------------------------------------------------------------------------------------------

print("\n\n<------ disk deployment email ------>\n")
print("To: GRIDPP-UB\@JISCMAIL.AC.UK")
print("Subject: Disk Deployment at RAL ({0} {1})".format(prevMonthName, str(prevYear)))

print("\n  Here is some information about disk deployment at RAL for {0}".format(prevMonthName))
print("{0}. Units are TB (10^12 B).".format(str(prevYear)))

print("        Experiment      Allocation      Deployment Echo")
print("\n        ALICE            %9.1f       \n" % (alloc["ALICE"]))
print("        ATLAS            %9.1f       %9.1f\n" % (alloc["ATLAS"], depl["ATLAS_echo"]))
print("        CMS              %9.1f       %9.1f\n" % (alloc["CMS"], depl["CMS_echo"]))
print("        LHCb             %9.1f       %9.1f\n" % (alloc["LHCb"], depl["LHCb_echo"]))
print("\n")
print("        LHC Total        %9.1f       %9.1f\n" % (alloc["lhc_total"], depl["lhc_total_echo"]))
print("\n")
print("        Non-LHC          %9.1f\n" % (alloc["Others"]))
print("\n")
print("        Total            %9.1f       %9.1f\n" % (alloc["total"], depl["total_echo"]))

print("\n")
print("References:")

print("    Tier-1 Schedule:")

print("\n        http://www.gridpp.rl.ac.uk/schedule/schedule.xls")

print("\n<------ end text for disk deployment email ------>\n")
