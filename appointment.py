import requests
import json
from datetime import date
import os
import mysql.connector
import time

class dataExchange : 
    
    def __init__(self) -> None:
        self.email = "xxxxxxxxxx"
        self.password = "xxxx"
        self.baseUrl = "https://dataexchange.xxxxx.com/api/v1/"
        self.limit = 50
        self.startDate = date.today()

    def authorization(self) :
        payload = json.dumps({
                    "email": self.email,
                    "password": self.password
                })
        headers = {
                    'Content-Type': 'application/json'
                }
        url = self.baseUrl+"user/sign-in"
        response = requests.request("POST", url, headers=headers, data=payload)
        resp = json.loads(response.text)
        self.token = resp['token']

    def PrepareData(self,page=1):
        url = self.baseUrl+"data/HKSM/APPOINTMENT?limit="+str(self.limit)+"&page="+str(page)
        #print(url)
        headers = {
          'Authorization': 'Bearer '+self.token,
          'security-code': 'xxxxxxxxxxxxxxxxxxx'
        }

        response = requests.request("GET", url, headers=headers)

        resp = json.loads(response.text)
        data = resp.get('data',[])
        #saveData = []
        if(len(data) > 0):
            appointment_file = open("appointment"+str(self.startDate)+".txt", "a")
            appointment_data_file = open("appointment_data"+str(self.startDate)+".txt", "a")
            appointment_status_file = open("appointment_status"+str(self.startDate)+".txt", "a")
            appointment_user_file = open("appointment_user"+str(self.startDate)+".txt", "a")
            #t = 1
            for k in data :
                # t=t+1
                #f print(t)
                createdBy = ''
                createdBy = ''
                if('createdBy' in k):
                    createdBy = k['createdBy']
                if('createdByName' in k):
                    createdByName = k['createdByName']
                invoiceData = str(k['_id'])+';'+str(k['flag'])+';'+str(k['id'])+';'+str(k['statusName'])+';'+str(k['customerId'])+';'+str(k['leadId'])+';'+str(k['outletId'])+';'+str(k['startDateTime'])+';'+str(k['endDateTime'])+';'+str(k['createdDateTime'])+';'+str(k['sourceId'])+';'+str(k['sourceName'])+';'+str(k['updated_at'])+';'+str(k['created_at'])+';'+str(createdBy)+';'+str(createdByName)
                appointment_file.write(invoiceData+'\n')
                appointment_id = k['id']
                if(len(k['status']) > 0):
                    #print(k['items'])
                    for p in k['status'] :
                        invoice_item = p['id']+';'+str(p['statusId'])+';'+str(p['statusName'])+';'+str(p['time'])+';'+str(p['userId'])+';'+str(p['userName'])+';'+appointment_id
                        appointment_status_file.write(invoice_item+'\n')
                
                if(len(k['items']) > 0):
                    #print(k['items'])
                    for j in k['items'] :
                        invoice_item = str(j['flag'])+';'+str(j['id'])+';'+str(j['itemId'])+';'+str(j['treatmentCategory'])+';'+str(j['sku'])+';'+str(j['therapistsRequested'])+';'+appointment_id
                        appointment_data_file.write(invoice_item+'\n')

                    if(type(j['users']) is list and len(j['users']) > 0):
                       
                        for m in j['users'] :
                            invoice_item = str(m['flag'])+';'+str(m['id'])+';'+str(m['userId'])+';'+str(m['userName'])+';'+appointment_id
                            appointment_user_file.write(invoice_item+'\n')

            #print(resp['pagination'])         
            if(resp['pagination']['lastPage'] > 1 and page != resp['pagination']['lastPage']) :
                page = page+1
                #print(page)
                self.PrepareData(page)
            else :
                #time.sleep(2)
                appointment_file.close()
                appointment_data_file.close()
                appointment_status_file.close()
                appointment_user_file.close()
               
                #os.system(conn)
               
                
        
    def insertData(self):
        connection = mysql.connector.connect(
                        host="host",
                        user="user",
                        password="password",
                        database = "db",
                        auth_plugin='mysql_native_password'
                    )
        cursor = connection.cursor()
        appointment_query = "LOAD DATA LOCAL INFILE 'appointment"+str(self.startDate)+".txt' INTO TABLE dataexchange_appointment FIELDS TERMINATED BY ';' (_id,flag,id,statusname,customerId,leadId,outletId,startDateTime,endDateTime,createdDateTime,sourceId,sourceName,updated_at,created_at,createdBy,createdByName);";
        appointment_item_query = "LOAD DATA LOCAL INFILE 'appointment_data"+str(self.startDate)+".txt' INTO TABLE dataexchange_appointment_data FIELDS TERMINATED BY ';' (flag,id,itemId,treatmentCategory,sku,therapistsRequested,appointment_id);";

        appointment_status_query = "LOAD DATA LOCAL INFILE 'appointment_status"+str(self.startDate)+".txt' INTO TABLE dataexchange_appointment_status FIELDS TERMINATED BY ';' (id,statusId,statusName,time,userId,userName,appointment_id);";
        appointment_users_query = "LOAD DATA LOCAL INFILE 'appointment_user"+str(self.startDate)+".txt' INTO TABLE dataexchange_appointment_user FIELDS TERMINATED BY ';' (flag,id,userId,userName,appointment_id);";
        
        #conn = f"mysql -h localhost -P 3306 -u root -proot -e \"{query}\"; "
        cursor.execute( appointment_query )
        cursor.execute( appointment_item_query )

        cursor.execute( appointment_status_query )
        cursor.execute( appointment_users_query )
        connection.commit()
        #print(cursor)
        print(cursor.rowcount, "record inserted.")
        exit()

dataexchange = dataExchange()
dataexchange.authorization()
dataexchange.PrepareData()
dataexchange.insertData()
        
