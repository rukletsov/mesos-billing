
import signal
import sys
import time
import datetime
import requests
import json
from threading import Thread

from influxdb import InfluxDBClient


# Globals.
shutting_down = False
last_cpus_value = 0

# Mesos constants.
mesos_endpoint = 'http://172.18.6.178:5050/accounting'
mesos_fetch_interval = 5

# InfluxDB constants.
user = 'user'
password = 'user'
dbname = 'demo1'
dbuser = 'dbuser'
dbuser_password = 'dbuser'
influx_host = 'localhost'
influx_port = '8086'


def shutdown(signal, frame):
    global shutting_down

    print "influx-exporter is shutting down"
    shutting_down = True


def fetch_mesos():
    r = requests.get(mesos_endpoint)
    data = r.json()
    return data['accounting']



def process_accounting_data(data):
    global last_cpus_value

    # Sort according to the timestamp (should not be necessary in future versions)
    data.sort(key=lambda x: x['timestamp'])

    for event in data:
        # For now drop is not cpus.
        if event['resource']['cpus'] == 0:
            continue

        if event['what'] == "TASK_STARTED":
            new_value = last_cpus_value + event['resource']['cpus']
        elif event['what'] == "TASK_TERMINATED":
            new_value = last_cpus_value - event['resource']['cpus']

        last_cpus_value = new_value
        print "STORE {} {}: {} [{}]" \
            .format('cpus', event['timestamp'], new_value, event['framework_id'])
        put_datapoint(event['timestamp'], 'cpus', event['framework_id'], new_value)


def fetch_infinitely():
    global shutting_down

    while True:
        if shutting_down:
            break

        data = fetch_mesos()
        process_accounting_data(data)

        time.sleep(mesos_fetch_interval)


# Puts a datapoint into the InfluxDB
def put_datapoint(timestamp, measurement, framework_id, value):
    json_body = [
        {
            "measurement": measurement,
            "tags": {
                "framework_id": framework_id
            },
            "time": int(timestamp),
#            "time": datetime.datetime.utcnow(),
            "fields": {
                "value": float(value)
            }
        }
    ]
    client.write_points(json_body, time_precision='s')


#
# Execution entry point:
#
if __name__ == "__main__":

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, shutdown)

    client = InfluxDBClient(influx_host, influx_port, user, password, dbname)

    print("Drop database: " + dbname)
    client.drop_database(dbname)

    print("Create database: " + dbname)
    client.create_database(dbname)

#     data = fetch_mesos()
#     process_accounting_data(data)

    fetch_thread = Thread(target = fetch_infinitely, args = ())
    fetch_thread.start()

    while fetch_thread.is_alive():
        time.sleep(1)

    print "Goodbye!"
    sys.exit(0)
