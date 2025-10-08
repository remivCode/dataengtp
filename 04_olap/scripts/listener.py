import psycopg2
import requests
import json
import time

AIRFLOW_API = "http://localhost:8080/api/v2/dags/customer/dagRuns"
AUTH = ('airflow', 'airflow')

def main():
    conn = psycopg2.connect(
        dbname="mydb",
        user="myuser",
        password="mypass",
        host="db1",
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN airflow_event;")

    print("Listening for DB events...")
    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop()
            payload = json.loads(notify.payload)
            print("Change detected:", payload)
            response = requests.post(AIRFLOW_API, auth=AUTH, json={"conf": payload})
            print("Airflow response:", response.status_code)
        time.sleep(1)

if __name__ == "__main__":
    main()