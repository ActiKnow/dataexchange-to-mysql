import requests
import json
from datetime import date
import os
import mysql.connector
import time
from functools import reduce

class dataExchange : 
    
    def __init__(self) -> None:
        self.email = "xxxxxxxxxxxxx"
        self.password = "xxxx"
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
        url = self.baseUrl+"data/HKSM/INVOICE?limit="+str(self.limit)+"&page="+str(page)
        #print(url)
        headers = {
          'Authorization': 'Bearer '+self.token,
          'security-code': 'xxxxxxxxxxxxxxxxxx'
        }

        response = requests.request("GET", url, headers=headers)

        resp = json.loads(response.text)
        data = resp.get('data',[])
        #saveData = []
        if(len(data) > 0):
            invoice_file = open("invoice"+str(self.startDate)+".txt", "a")
            invoice_item_file = open("invoice_items"+str(self.startDate)+".txt", "a")
            #t = 1
            for k in data :
                # t=t+1
                #f print(t)
                invoiceData = k['_id']+';'+str(k['flag'])+';'+str(k['id'])+';'+str(k['type'])+';'+str(k['refInvoiceId'])+';'+str(k['invoiceNum'])+';'+str(k['customerId'])+';'+str(k['customerMembershipId'])+';'+str(k['customerMembershipName'])+';'+str(k['invoicedDateTime'])+';'+str(k['createdBy'])+';'+str(k['createdDateTime'])+';'+str(k['itemsExpiryDate'])+';'+str(k['isVoided'])+';'+str(k['voidedDateTime'])+';'+str(k['isFullyPaid'])+';'+str(k['fullyPaidDate'])+';'+str(k['isFullyUtilised'])+';'+str(k['discountAmount'])+';'+str(k['totalExclTax'])+';'+str(k['deposit'])
                invoice_file.write(invoiceData+'\n')
                if(len(k['items']) > 0):
                    #print(k['items'])
                    for p in k['items'] :
                        promoCode = ''
                        
                        if 'promoCode' in p:
                            promoCode = p['promoCode'][1:][:-1]
                        
                        invoice_item = p['id']+';'+str(p['invoiceId'])+';'+str(p['itemId'])+';'+str(p['itemSkuCode'])+';'+str(p['itemName'])+';'+str(p['itemType'])+';'+str(p['categoryId'])+';'+str(p['categoryName'])+';'+str(p['unitPrice'])+';'+str(p['salesUnitPrice'])+';'+str(p['discountType'])+';'+str(p['discountAmount'])+';'+str(p['discountActualAmount'])+';'+str(p['paidQty'])+';'+str(p['focQty'])+';'+str(p['lineTotal'])+';'+str(p['isTaxable'])+';'+str(p['displayOrder']+promoCode)
                        invoice_item_file.write(invoice_item+'\n')
            #print(resp['pagination'])         
            if(resp['pagination']['lastPage'] > 1 and page != resp['pagination']['lastPage']) :
                page = page+1
                #print(page)
                self.PrepareData(page)
            else :
                #time.sleep(2)
                invoice_file.close()
                invoice_item_file.close()
               
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
        invoice_query = "LOAD DATA LOCAL INFILE 'invoice"+str(self.startDate)+".txt' INTO TABLE invoice FIELDS TERMINATED BY ';' (_id,flag,id,type,refInvoiceId,invoiceNum,customerId,customerMembershipId,customerMembershipName,invoicedDateTime,createdBy,createdDateTime,itemsExpiryDate,isVoided,voidedDateTime,isFullyPaid,fullyPaidDate,isFullyUtilised,discountAmount,totalExclTax,deposit);";
        invoice_item_query = "LOAD DATA LOCAL INFILE 'invoice_items"+str(self.startDate)+".txt' INTO TABLE invoice_items FIELDS TERMINATED BY ';' (id,invoiceId,itemId,itemSkuCode,itemName,itemType,categoryId,categoryName,unitPrice,salesUnitPrice,discountType,discountAmount,discountActualAmount,paidQty,focQty,lineTotal,isTaxable,displayOrder+promoCode);";
        
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
        
