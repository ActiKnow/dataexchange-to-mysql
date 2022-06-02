import requests
import json
from datetime import date
import os
import mysql.connector
import time

class dataExchange : 
    
    def __init__(self) -> None:
        self.email = "XXXXXXXXXXXXXXXXXXXXX"
        self.password = "XXXXXXXXX"
        self.baseUrl = "https://dataexchange.xxxx.com/api/v1/"
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
        url = self.baseUrl+"data/HKSM/CUSTOMER?limit="+str(self.limit)+"&page="+str(page)
        #print(url)
        headers = {
          'Authorization': 'Bearer '+self.token,
          'security-code': 'XXXXXXXXXXXXXXXXXX'
        }

        response = requests.request("GET", url, headers=headers)

        resp = json.loads(response.text)
        data = resp.get('data',[])
        #saveData = []
        if(len(data) > 0):
            customer_file = open("customer"+str(self.startDate)+".txt", "a")
            customer_data_file = open("customer_data"+str(self.startDate)+".txt", "a")
            #t = 1
            for k in data :
                # t=t+1
                #f print(t)
                invoiceData = str(k['_id'])+';'+str(k['flag'])+';'+str(k['id'])+';'+str(k['firstName'])+';'+str(k['lastName'])+';'+str(k['displayName'])+';'+str(k['contact'])+';'+str(k['dob'])+';'+str(k['gender'])+';'+str(k['email'])+';'+str(k['membershipId'])+';'+str(k['membershipName'])+';'+str(k['referralId'])+';'+str(k['enrollmentDate'])+';'+str(k['isActive'])+';'+str(k['activationDate'])
                customer_file.write(invoiceData+'\n')
                customer_id = k['id']
                if(len(k['data']) > 0):
                    #print(k['items'])
                    for p in k['data'] :
                        invoice_item = str(p['id'])+';'+str(p['fieldId'])+';'+str(p['fieldName'])+';'+str(p['value'])+';'+str(p['displayOrder'])+';'+customer_id
                        customer_data_file.write(invoice_item+'\n')

            #print(resp['pagination'])         
            if(resp['pagination']['lastPage'] > 1 and page != resp['pagination']['lastPage']) :
                page = page+1
                #print(page)
                self.PrepareData(page)
            else :
                #time.sleep(2)
                customer_file.close()
                customer_data_file.close()
               
                #os.system(conn)
               
                
        
    def insertData(self):
        connection = mysql.connector.connect(
                        host="host",
                        user="User",
                        password="password",
                        database = "db",
                        auth_plugin='mysql_native_password'
                    )
        cursor = connection.cursor()
        invoice_query = "LOAD DATA LOCAL INFILE 'customer"+str(self.startDate)+".txt' INTO TABLE dataexchange_customer FIELDS TERMINATED BY ';' (_id,flag,id,firstName,lastName,displayName,contact,dob,gender,email,membershipId,membershipName,referralId,enrollmentDate,isActive,activationDate);";
        invoice_item_query = "LOAD DATA LOCAL INFILE 'customer_data"+str(self.startDate)+".txt' INTO TABLE dataexchange_customer_data FIELDS TERMINATED BY ';' (id,fieldId,fieldName,value,displayOrder,customer_id);";
        
        #conn = f"mysql -h localhost -P 3306 -u root -proot -e \"{query}\"; "
        cursor.execute( invoice_query )
        cursor.execute( invoice_item_query )
        connection.commit()
        #print(cursor)
        print(cursor.rowcount, "record inserted.")
        exit()

dataexchange = dataExchange()
dataexchange.authorization()
dataexchange.PrepareData()
dataexchange.insertData()
        
