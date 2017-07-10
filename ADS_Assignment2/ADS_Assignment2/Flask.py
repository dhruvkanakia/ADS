import os.path

from flask import Flask, Response, request, render_template
import cf_deployment_tracker
import ibm_db
from math import sin, cos, sqrt, atan2, radians
import ast
import ibm_db_dbi
import calculate

cf_deployment_tracker.track()
app = Flask(__name__)

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
# ...
# app = Flask('myapp', template_folder=tmpl_dir)


# Enter the values for you database connection
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
port = int(os.getenv('PORT', 8080))
@app.route("/all")
def hello():
    print(tmpl_dir)


    curs.execute("SELECT * FROM DASH14383.CLEAN fetch first 10 row only")
    results = curs.fetchall()

    file = open('results.html', 'w')

    message = """<html> <head> <meta http-equiv="Content-Type" 
                content="text/html; charset=UTF-8"><title>Results Page</title>
                </head> <body><h1><u> Here are the search results </u></h1><br/>
                <table> 
                 <thead><tr>
                <th>Location Type</th>
                <th>Latitude</th>
                <th>Longitude</th>
               </tr>
                </thead>
                <tbody>"""
    message3 = """
                       </tbody>

                       </table>
                     
                       </body>
                       </html>"""
    # for result in results:
    message2 = show(results)
    final = message + message2 + message3
    file.write(final)
    file.close()
    content = get_file('results.html')
    return Response(content, mimetype="text/html")


def show(results):
    # test=[]
    messsage = ""
    for test in results:
        # test.append(result)

        messsage2 = """ <tr>
         <td>""" + test[14] + """</td>
         <td>""" + test[19] + """</td>
         <td>""" + test[21] + """</td>
         </tr> """
        messsage += messsage2
    print(messsage)
    return messsage


def root_dir():  # pragma: no cover
    return os.path.abspath(os.path.dirname(__file__))


def get_file(filename):  # pragma: no cover
    try:
        src = os.path.join(root_dir(), filename)
        return open(src).read()
    except IOError as exc:
        return str(exc)

@app.route('/getLocationDetails',methods=['POST'])
def getLocationDetails():
    longitideGiven = float(ast.literal_eval(request.form.get("longitude")))
    latitudeGiven = float(ast.literal_eval(request.form.get("latitude")))
    topten=calculate.getAllLocations(latitudeGiven,longitideGiven)
    searchResults(topten)
    content = get_file("resultSet.html")
    return Response(content, mimetype="text/html")

def searchResults(topten):
    file1 = open('resultSet.html', 'w')

    message = """<html> <head> <meta http-equiv="Content-Type" 
                   content="text/html; charset=UTF-8"><title>Results Page</title>
                   </head> <body><h1><u> Here are the search results </u></h1><br/>
                   <table> 
                    <thead><tr>
                   
                   <th>Latitude</th>
                   <th>Longitude</th>
                   <th>Distance</th>
                  </tr>
                   </thead>
                   <tbody>"""
    message3 = """
                          </tbody>

                          </table>
                          <a href="/">Click here to go back to main page</a>
                          </body>
                          </html>"""
    # for result in results:
    message2 = calculate.buildTable(topten)
    final = message + message2 + message3
    file1.write(final)
    file1.close()


@app.route('/')
def metrics():
    content = get_file('welcome.html')
    return Response(content, mimetype="text/html")


if __name__ == "__main__":
    # app.run()
     app.run(host='0.0.0.0', port=port, debug=True)