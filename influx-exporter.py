
import signal
import sys
import time
import datetime
from threading import Thread

from influxdb import InfluxDBClient


# Global constants.
shutting_down = False

# Mesos constants.

# InfluxDB constants.
user = 'user'
password = 'user'
dbname = 'test1'
dbuser = 'dbuser'
dbuser_password = 'dbuser'
host = 'localhost'
port = '8086'


def shutdown(signal, frame):
    global shutting_down

    print "influx-exporter is shutting down"
    shutting_down = True


def fetch_infinitely():
    global shutting_down

    while True:
        if shutting_down:
            break

        time.sleep(1)
        print "Putting datapoint"
        put_datapoint()


# Puts a datapoint into the InfluxDB
# The JSON format is
#     json_body = [
#         {
#             "measurement": "cpus",
#             "tags": {
#                 "id": "uuid1",
#                 "frameworkId": "2015-...",
#                 "what": "TASK_STARTED"
#             },
#             "time": datetime.datetime.utcnow(),
#             "fields": {
#                 "value": 0.64
#             }
#         }
#     ]
def put_datapoint():
    json_body = [
        {
            "measurement": "cpus",
            "tags": {
                "id": "uuid1",
                "frameworkId": "2015-...",
                "what": "TASK_STARTED"
            },
            "time": datetime.datetime.utcnow(),
            "fields": {
                "value": 0.64
            }
        }
    ]
    client.write_points(json_body)


#
# Execution entry point:
#
if __name__ == "__main__":

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, shutdown)

    client = InfluxDBClient(host, port, user, password, dbname)

    print("Create database: " + dbname)
    client.drop_database(dbname)
    client.create_database(dbname)

    fetch_thread = Thread(target = fetch_infinitely, args = ())
    fetch_thread.start()

    while fetch_thread.is_alive():
        time.sleep(1)

    print("Drop database: " + dbname)
    #client.drop_database(dbname)

    print "Goodbye!"
    sys.exit(0)
