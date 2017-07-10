import ibm_db
from math import sin, cos, sqrt, atan2, radians
# from cmath import
import ast
import ibm_db_dbi


class CalculateNearestLocation:
    def __init__(self,airconditiontype, latitude, longitude, distanceBWPoints):
        self.airconditiontype = airconditiontype
        self.longitude = longitude
        self.latitude = latitude
        self.distanceBWPoints = distanceBWPoints

    def __lt__(self, other):
        return self.distanceBWPoints < other.distanceBWPoints


dsn_driver = "IBM DB2 ODBC DRIVER"
dsn_database = "BLUDB"  # e.g. "BLUDB"
dsn_hostname = "dashdb-entry-yp-dal09-08.services.dal.bluemix.net"  # e.g.: "awh-yp-small03.services.dal.bluemix.net"
dsn_port = "50000"  # e.g. "50000"
dsn_protocol = "TCPIP"  # i.e. "TCPIP"
dsn_uid = "dash14383"  # e.g. "dash104434"
dsn_pwd = "4$7ojHBqQq#T"  # e.g. "7dBZ3jWt9xN6$o0JiX!m"

dsn = (
    "DRIVER={{IBM DB    2 ODBC DRIVER}};"
    "DATABASE={0};"
    "HOSTNAME={1};"
    "PORT={2};"
    "PROTOCOL=TCPIP;"
    "UID={3};"
    "PWD={4};").format(dsn_database, dsn_hostname, dsn_port, dsn_uid, dsn_pwd)

conn = ibm_db.connect(dsn, "", "")
pconn = ibm_db_dbi.Connection(conn)
# df = pd.read_sql('SELECT * FROM DASH14383.DH', pconn)
curs = pconn.cursor()


def calculateDistance(latorigin, longorigin, latdest, longdest):
    Radius = 6373.0

    if (latorigin == ""):
        latorigin = 0.0
    print(latorigin)
    print(type(latorigin))
    latdest = radians(float(latdest))
    longdest = radians(abs(float(longdest)))
    latorigin1 = radians(float(latorigin))
    longorigin1 = radians(abs(float(longorigin)))
    latSeperation = (latdest) - (latorigin1)
    longSeperation = (longdest) - (longorigin1)
    a = (sin(latSeperation / 2)) ** 2 + cos(latorigin1) * cos(latdest) * (sin(longSeperation / 2)) ** 2

    ab = abs(a)
    # print(ab)
    a1 = sqrt(ab)
    a2 = sqrt(1 - ab)
    c = 2 * atan2(a1, a2)
    distance = c * Radius
    return distance


def getAllLocations(latgiven, longtgiven):
    topten = []
    curs.execute("SELECT * FROM DASH14383.CLEAN")
    lists = curs.fetchall()
    latgivenabs = float(latgiven)
    longtgivenabs = float(longtgiven)
    for row in lists:
        latitude = float((row[19]))
        longitude = float((row[21]))
        distanceBWPoints = calculateDistance(latgivenabs, longtgivenabs, latitude, longitude)
        if len(topten) < 10:
            locationDetails = CalculateNearestLocation(row[0], latitude, longitude, distanceBWPoints)
            topten.append(locationDetails)
            topten.sort()

        elif distanceBWPoints < topten[9].distanceBWPoints:
            print(distanceBWPoints);
            print(topten[9].distanceBWPoints)
            del topten[9]
            locationDetails = CalculateNearestLocation(row[0],latitude, longitude, distanceBWPoints)
            topten.append(locationDetails)
            topten.sort()
        else:
            continue
    return topten


def buildTable(topten):
    messsage = ""
    for CalculateNearestLocation in topten:
        # test.append(result)

        messsage2 = """ <tr>
              
        <td> """ +  str(CalculateNearestLocation.latitude) +""" </td> 
        <td> """ +  str(CalculateNearestLocation.longitude) +""" </td >
         <td> """ +  str(CalculateNearestLocation.distanceBWPoints) +""" </td>
             </tr> """
        messsage += messsage2
    print(messsage)
    return messsage
