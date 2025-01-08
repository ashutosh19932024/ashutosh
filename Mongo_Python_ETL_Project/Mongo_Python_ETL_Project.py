#!/usr/bin/env python
# coding: utf-8

# In[2555]:


import pandas as pd


# In[2556]:


import pymongo
import pprint
import json
import warnings
#import config as cn
warnings.filterwarnings('ignore')


# In[2557]:


from datetime import date, timedelta

last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)


# In[2558]:


#first_day


# In[2559]:


#first_day='02/01/2021'


# In[2560]:


#today='02/28/2021'


# In[2561]:


first_day=start_day_of_prev_month


# In[2562]:


today=last_day_of_prev_month


# In[2563]:


today2=last_day_of_prev_month+timedelta(days=1)


# In[2564]:


#last_day_of_prev_month


# In[2565]:


#today1


# In[2566]:


first_day1=first_day.strftime('%Y-%d-%m %H:%M:%S')


# In[2567]:


today1=today.strftime('%Y-%d-%m %H:%M:%S')


# In[2568]:


first_day=first_day.strftime('%m/%d/%Y')


# In[2569]:


today=today.strftime('%m/%d/%Y')


# In[2570]:


today2=today2.strftime('%m/%d/%Y')


# In[2571]:


def mongodb():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    mongodb=config['mongodb']
    #print(mongodb)
    return(mongodb)
mongodb()


# In[2572]:


def database_config():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    database=config['database']
    #print(mongodb)
    return(database)
database_config()


# In[2573]:


client = pymongo.MongoClient(mongodb())


# In[2574]:


database = client[database_config()]


# In[2575]:


db = client[database_config()]


# In[2576]:


#client = pymongo.MongoClient(mongodb())

# get the database
#database = client[database()]
NonMonitoringApiLogs = database.get_collection("NonMonitoringApiLogs")
NonMonitoringApiLogs.aggregate(
    [{"$match":{
        "Type":"Response",
            "Datetime":{
            "$gte":first_day1,
            "$lte":today1
        }
       }
       },{
        "$project":{
            "_id" :"$_id",
            "ResponseCode":"$ResponseCode",
            "clientid":"$CIMvalue.ClientId",
            "InstituteId":"$CIMvalue.InstituteId", 
            "Datetime":"$Datetime"
            
        }
        
    },{"$out" :"NonMonitoringApiLogs_Billing"}
        
        ])


# In[2577]:


#first_day1='2021-02-01 00:00:01'


# In[2578]:


#today1='2021-02-28# 00:00:01'


# In[2579]:


APIRecord = database.get_collection("APIRecord")

APIRecord.aggregate(
    [{
        "$match":{
            "Type":"Response",
            "Datetime":{
            "$gte":first_day1,
            "$lte":today1
        }
       }
       },{
        
        "$project":{
            "_id" :"$_id",
            "ResponseCode":"$ResponseCode",
            "clientid":"$CIMvalue.ClientId",
            "InstituteId":"$CIMvalue.InstituteId",
            "Datetime":"$Datetime"
        }
       
    }
    ,{"$out" :"APIRecord_Billing"}
        
        ])


# In[2580]:


#APIRecord


# In[2581]:


NonMonitoringApiLogs_Billing = database.get_collection("NonMonitoringApiLogs_Billing")
Client = database.get_collection("Client")
NonMonitoringApiLogs_Billing.aggregate(
    [{
       "$lookup": {
                  "from": "Client",
                  "localField": "clientid",
                  "foreignField": "ClientID",
                  "as" : "client_docs"
                }
       },
       { "$match": {
           "client_docs": {
               "$ne":[]
           }
       }
           
       },
       {
           "$addFields": {
               "client_docs":{
                   "$arrayElemAt": ["$client_docs",0]
               }
           }
       },{
        "$project":{
            "_id" :"$_id",
            "ResponseCode":"$ResponseCode",
            "Datetime":"$Datetime",
            "InstituteId":"$InstituteId", 
            "ClientName":"$client_docs.ClientName", 
           "Datetime":"$Datetime"
        }  
        },
        {"$out" :"NonMonitoringApiLogs_Billing"}
    
       
    ])


# In[2582]:


APIRecord_Billing = database.get_collection("APIRecord_Billing")
Client = database.get_collection("Client")
APIRecord_Billing.aggregate(
    [{
       "$lookup": {
                  "from": "Client",
                  "localField": "clientid",
                  "foreignField": "ClientID",
                  "as" : "client_docs"
                }
       },
       { "$match": {
           "client_docs": {
               "$ne":[]
           }
       }
           
       },
       {
           "$addFields": {
               "client_docs":{
                   "$arrayElemAt": ["$client_docs",0]
               }
           }
       },{
        "$project":{
            "_id" :"$_id",
            "ResponseCode":"$ResponseCode",
            "Datetime":"$Datetime",
            "InstituteId":"$InstituteId", 
            "ClientName":"$client_docs.ClientName", 
           
        }  
        },
        {"$out" :"APIRecord_Billing"}
    
       
    ])


# In[2583]:


rpt_billing = database.get_collection("rpt_billing")


# In[2584]:


rpt_billing.delete_many({})


# In[2585]:


from pymongo import MongoClient


# In[2586]:


client = pymongo.MongoClient(mongodb())


# In[2587]:


#client


# In[2588]:


#client = MongoClient(host="localhost", port=27017)


# In[2589]:


db = client[database_config()]


# In[2590]:


#db


# In[2591]:


import pprint


# In[2592]:


NonMonitoringApiLogs_Billing=db.NonMonitoringApiLogs_Billing


# In[2593]:


l=[]
for doc in NonMonitoringApiLogs_Billing.find():
    
    #pprint.pprint(doc)
    l.append(doc)
   


# In[2594]:


#print(l)


# In[2595]:


df=pd.DataFrame(l,columns= ['_id', 'Datetime','InstituteId','ClientName'])


# In[2596]:


#print(df)


# In[2597]:


Client=db.Client


# In[2598]:


l1=[]
for doc in Client.find():
    
   # pprint.pprint(doc)
    l1.append(doc)


# In[2599]:


df2=pd.DataFrame(l1,columns= ['_id', 'ClientID','InstitutionID','InstitutionName','ClientName'])


# In[2600]:


#print(df2)


# In[2601]:


df3=df2.merge(df, on='ClientName', how='left')
#print(df3)


# In[2602]:


#df3


# In[2603]:


df3['Datetime'] = pd.to_datetime(df3['Datetime'], format='%Y-%m-%d %H:%M:%S.%f')


# In[2604]:


df3['Datetime'] = pd.to_datetime(df3['Datetime'], format='%m/%d/%Y')


# In[2605]:


#today


# In[2606]:


#first_day


# In[2607]:


#df4=df3["Datetime"].between('04/01/2021', '05/01/2021',inclusive = True)  


# In[2608]:


#df3[df4]


# In[2609]:


df4=df3[(df3.Datetime >= first_day) & (df3.Datetime <= today2)]


# In[2610]:


#df4


# In[2611]:


#filter = df3["Datetime"]="2021-04-29 23:59:59.999"


# In[2612]:


#df3.where(filter, inplace = True)


# In[2613]:


df5=df2.merge(df4, on='ClientName', how='left')


# In[2614]:


#print(df5)


# In[2615]:


df6=df5.set_index(["_id_y", "ClientID_x"]).count(level="ClientID_x")


# In[2616]:


#print(df6)


# In[2617]:


df7=df6.reset_index()


# In[2618]:


#type(df7)


# In[2619]:


#df7[['ClientID_x','_id_x']]


# In[2620]:


df7['datefrom']= first_day


# In[2621]:


#df7


# In[2622]:


df7['Dateto']= today


# In[2623]:


df7['Category']='Non Monetary Transactions'


# In[2624]:


#print(df7)


# In[2625]:


df8=df7[['ClientID_x','_id_x','datefrom','Dateto','Category']]


# In[2626]:


df18=df8.rename(columns={"ClientID_x":"ClientID"})


# In[2627]:


#df8


# In[2628]:


df19=df18.merge(df2, on='ClientID', how='inner')


# In[2629]:


df19=df19[['ClientID','_id_x','datefrom','Dateto','Category','ClientName']]


# In[2630]:


#print(df19)


# In[2631]:


APIRecord_Billing=db.APIRecord_Billing


# In[2632]:


l3=[]
for doc in APIRecord_Billing.find():
    
    #pprint.pprint(doc)
    l3.append(doc)


# In[2633]:


df10=pd.DataFrame(l3,columns= ['_id', 'Datetime','InstituteId','ClientName','ClientID'])


# In[2634]:


#df10.head(1000)


# In[2635]:


df11=df2.merge(df10, on='ClientName', how='left')
#print(df11)


# In[2636]:


df11['Datetime'] = pd.to_datetime(df11['Datetime'], format='%Y-%m-%d %H:%M:%S.%f')


# In[2637]:


df11['Datetime'] = pd.to_datetime(df11['Datetime'], format='%m/%d/%Y')


# In[2638]:


df12=df11[(df11.Datetime > first_day) & (df11.Datetime <= today2)]


# In[2639]:


#print(df12)


# In[2640]:


df13=df2.merge(df12, on='ClientName', how='left')


# In[2641]:


#print(df13)


# In[2642]:


df14=df13.set_index(["_id_y", "ClientID"]).count(level="ClientID")


# In[2643]:


#print(df14)


# In[2644]:


df14=df14.reset_index()


# In[2645]:


df14['datefrom']= first_day
df14['Dateto']= today
df14['Category']='Monetary Transactions'


# In[2646]:


#today


# In[2647]:


#df14


# In[2648]:


df16=df14[['ClientID','_id_x','datefrom','Dateto','Category']]


# In[2649]:


df16=df16.rename(columns={"ClientID_x":"ClientID"})


# In[2650]:


df15=df16.merge(df2, on='ClientID', how='inner')


# In[2651]:


#df15


# In[2652]:


df15=df15[['ClientID','_id_x','datefrom','Dateto','Category','ClientName']]


# In[2653]:


#df15


# In[2654]:


df20=df15.merge(df19, on='ClientID', how='inner')


# In[2655]:


#df20


# In[2656]:


df21=df20[['ClientID','_id_x_x','_id_x_y','datefrom_x','Dateto_x','Category_x','Category_y','ClientName_x']]


# In[2657]:


#df21


# In[2658]:


df21=df21.rename(columns={"_id_x_x":"Monetary_Transactions_count","_id_x_y":"Non_Monetary_Transactions_count",
                         "datefrom_x":"datefrom","Dateto_x":"Dateto","Category_x":"Monetary_Category","Category_y"
                         :"Non_Monetary__Category","ClientName_x":"ClientName"})


# In[2659]:


#df21


# In[2660]:


rpt_billing_col = db["rpt_billing"]


# In[2661]:


#df21


# In[2662]:


import json


# In[2663]:


records = json.loads(df21.T.to_json()).values()


# In[2664]:


#records


# In[2665]:


db.rpt_billing.insert(records)


# In[2666]:


import requests
from requests_ntlm import HttpNtlmAuth
import time


# In[2667]:


def ssrsurl():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    ssrsurl=config['ssrsurl']
    #print(mongodb)
    return(ssrsurl)
ssrsurl()


# In[2668]:


def path():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    path=config['path']
    #print(mongodb)
    return(path)
path()


# In[2669]:


def username():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    username=config['username']
    #print(mongodb)
    return(username)
username()


# In[2670]:


def password():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    password=config['password']
    #print(mongodb)
    return(password)
password()


# In[2671]:


ssrsurl1=ssrsurl()


# In[2672]:


path=path()


# In[2673]:


username=username()


# In[2674]:


password=password()


# In[2675]:


rdl='?%2fTestACQReports%2fAcquiringBillingReportSummary%2fAcquiringBillingReportSummary&rs:Command=Render'


# In[2676]:


xls='&rs%3AFormat=Excel'


# In[2677]:


ClientID='&ClientID=25400001'


# In[2678]:


url=ssrsurl1+rdl+ClientID+xls


# In[2679]:


#url


# In[2680]:


from datetime import datetime
now = datetime.now()


# In[2681]:


import datetime
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
month=lastMonth.strftime('%B')


# In[2682]:


#month=now.strftime('%B')
year=now.year


# In[2683]:


year=str(year)


# In[2684]:


filename2='ACQBillingReport_25400001'


# In[2685]:


filename1=filename2+'_'+month+year


# In[2686]:


#username = username
#password = password
r = requests.get(url, verify=False,auth=HttpNtlmAuth(username, password))
timestr = time.strftime("%Y%m%d-%H%M%S")
#path='Y:\\'
#filename1=filename2+month+year
filename=path+filename1+'.xls'
if r.status_code == 200:
    with open(filename, 'wb') as out:
        for bits in r.iter_content():
            out.write(bits)


# In[ ]:


ClientID1='&ClientID=20000321'


# In[ ]:


url1=ssrsurl1+rdl+ClientID1+xls


# In[ ]:


filename3='ACQBillingReport_20000321'


# In[ ]:


filename1=filename3+'_'+month+year


# In[ ]:


#username = "ashutosh.singh"
#password = "@2021Happy"
r = requests.get(url1,verify=False, auth=HttpNtlmAuth(username, password))
#timestr = time.strftime("%Y%m%d-%H%M%S")
#path='Y:\\'
#filename1=filename2+month+year
filename=path+filename1+'.xls'
if r.status_code == 200:
    with open(filename, 'wb') as out:
        for bits in r.iter_content():
            out.write(bits)


# In[ ]:





# In[ ]:




