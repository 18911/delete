import w1thermsensor
import psycopg2
import time
import datetime
from datetime import date

sensor = w1thermsensor.W1ThermSensor()


def measurement(connection,cursor):
        print("measurement fun", flush=True)

        now = datetime.datetime.now()
        nowTime = datetime.time(now.hour,now.minute,now.second)
        nowDate = date(now.year,now.month,now.day)

        temp = sensor.get_temperature()
        print(nowDate," , ",nowTime,"  ","temperature: ", temp, flush=True)

        query = "SELECT id_temperature FROM temperature ORDER BY id_temperature DESC LIMIT 1"
        cursor.execute(query)
        response = cursor.fetchone()
        maxId = 0
        if(response == None):
                print("Response: none");
        else:
                print("Response: ", int(response[0]))
                maxId = int(response[0]) + 1

        query = "INSERT INTO temperature(id_temperature,id_device,t_value,m_date,m_time) VALUES(%s,%s,%s,%s,%s)"
        values = (maxId,1,temp,nowDate,nowTime);

        cursor.execute(query, values)
        connection.commit()

if __name__== '__main__':
        print("temperature docker", flush=True)
        print("Waiting 30s for db set up");
        time.sleep(30)
        #try:
        connection = psycopg2.connect(user = "bursztyn",password = "openflow",host = "k-db.kube.default.svc.cluster.local",port = "5432",database = "smarthomedb")
        cursor = connection.cursor()
        while True:
                measurement(connection,cursor)
                time.sleep(15)

        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
        #except (Exception, psycopg2.Error) as error :
                #print ("Error while connecting to PostgreSQL", error)
