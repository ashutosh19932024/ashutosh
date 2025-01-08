#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
#import pandas.io.sql as psql
import pyodbc
import json
import warnings
#import config as cn
warnings.filterwarnings('ignore')
import time
import datetime
import logging
import threading 
from threading import Thread
#import time
import smtplib


# In[3]:


def logpath():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    logpath=config['logpath']
    #print(mongodb)
    return(logpath)
print(logpath())


# In[535]:


# datetime.datetime.now() to get
# current date as filename.

filename = datetime.datetime.now()
f=filename.strftime('%d %B %Y')
f='schedule_log'+f
#f
path=logpath()
filename1=path+f+'.log'
#logging.basicConfig(filename=filename1, level=logging.INFO)


# In[536]:


logger = logging.getLogger("OSA")

logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(filename1, mode="a")#'a' for append you can use 'w' for write

formatter = logging.Formatter("%(asctime)s:  %(message)s",
                              "%Y-%m-%d %H:%M:%S")
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)


# In[537]:


logger.info("-------------------------------------------------------------------------------------------------------------------")


# In[538]:


logger.info("Code Version RD_Scheduler_0.2_11282022")


# In[539]:


# In[4]:


def server():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    server=config['server']
    #print(mongodb)
    return(server)
#logger.info(server())


# In[540]:



def database():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    database=config['database']
    #print(mongodb)
    return(database)
#logger.info(database())


# In[129]:


def databaseDW():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    databaseDW=config['databaseDW']
    #print(mongodb)
    return(databaseDW)
#logger.info(databaseDW())


# In[130]:


def UID():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    UID=config['UID']
    #print(mongodb)
    return(UID)
#logger.info(UID())


# In[131]:


def PWD():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    PWD=config['PWD']
    #print(mongodb)
    return(PWD)
#logger.info(PWD())



# In[541]:


# In[5]:


def SMTPSERVER():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    SMTPSERVER=config['SMTPSERVER']
    #print(mongodb)
    return(SMTPSERVER)
#logger.info(PWD())


# In[6]:


def FROM():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    FROM=config['FROM']
    #print(mongodb)
    return(FROM)
#logger.info(PWD())


# In[7]:


def TO():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    TO=config['TO']
    #print(mongodb)
    return(TO)
#logger.info(PWD())


# In[8]:


connection = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
cursor = connection.cursor()


# In[9]:


cursor1 = connection1.cursor()
rows1=cursor1.execute("select TOP(1) convert(varchar(23), cast(datenow as datetime), 121)  from udf_RPT_ReportGenGoAhead(NULL) WHERE datenow IS NOT NULL ORDER BY datenow" )
rows1 = cursor1.fetchall()
row1= ()
for row1 in rows1:
    row1

# In[135]:


DW=('('.join(row1))


# In[136]:


# In[10]:


rows=cursor.execute("select convert(varchar(23), cast(ProcDayend-1 as datetime), 121) as ProcDayStart from arsystemaccounts" )
rows = cursor.fetchall()
row= ()
for row in rows:
    row

# In[135]:


ProcDayStart=('('.join(row))


# In[136]:


# In[11]:


rows3=cursor.execute("select convert(varchar(23), cast(ProcDayend as datetime), 121) as ProcDayend from arsystemaccounts" )
rows3 = cursor.fetchall()
row3= ()
for row3 in rows3:
    row3

# In[135]:


ProcDayend=('('.join(row3))


# In[136]:


# In[12]:


rows4=cursor.execute("select convert(varchar(10), cast(ProcDayStart as datetime), 101) as ProcDayStart from arsystemaccounts" )
rows4 = cursor.fetchall()
row4= ()
for row4 in rows4:
    row4

# In[135]:


ProcDayStart1=('('.join(row4))


# In[136]:


# In[13]:


logger.info("-------------------------------------------------------------------------------------------------------------------")


# In[14]:


logger.info("DW DateTime:-"+DW)


# In[15]:


logger.info("ProcDayStart DateTime:-"+ProcDayStart)


# In[16]:


logger.info("ProcDayend DateTime:-"+ProcDayend)


# In[17]:


logger.info("-------------------------------------------------------------------------------------------------------------------")


# In[18]:


sql = """exec Generate_RDScheduler_RemaningJobCount"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)
rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    row1
#new_lst2=('('.join(row1))
#new_lst1 
new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)


# In[543]:


def current():
        connection_current = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_current.cursor()
        #with lock:
        sql = """        EXEC GenerateCurrentBalanceAsOnDate_OptTables_SCH @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto)
        cursor.execute(sql, params)
        connection_current.commit() 
        #mutex.release()
        #connection1.commit()


# In[544]:


def status():
        connection_status = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_status.cursor()
        #global mutex
        sql = """        EXEC GenerateStatusAsOnDate_OptTables_withdate_sch @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto)
        cursor.execute(sql, params)
        connection_status.commit() 
        #mutex.release()
        #connection1.commit()


# In[545]:


def ProductTransfer():
        connection_ProductTransfer = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_ProductTransfer.cursor()
        #global mutex
        sql = """    EXEC GenerateProductTransferLinks_SH @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto)
        cursor.execute(sql, params)
        connection_ProductTransfer.commit()  
        #mutex.release()
        #connection1.commit()


# In[546]:


def ProductTransfer_ChildLess():
        connection_ProductTransfer_ChildLess = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_ProductTransfer_ChildLess.cursor()
        #global mutex
        sql = """    EXEC GenerateBaseAccountsFromXREF_PT @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto)
        cursor.execute(sql, params)
        connection_ProductTransfer_ChildLess.commit() 
        #mutex.release()
        #connection1.commit()


# In[547]:


def ActiveCloseBranches():
        connection_ActiveCloseBranches = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_ActiveCloseBranches.cursor()
        #global mutex
        sql = """    EXEC GenerateActiveCloseBranches_ProgramCount_SCH @InstitutionID = ?, @ProductID = ?, @Channelid = ?,@DateFrom=?,@DateTo=?,@BranchName=?,@BranchFlag=?,@userid=?
        """
        params = (DFAID1,0,0,dateto,dateto,'','','')
        cursor.execute(sql, params)
        #mutex.release()
        connection_ActiveCloseBranches.commit() 
        
       # connection1.commit()


# In[548]:


# In[19]:


if ProcDayStart <= DW and new_lst1 !=0:
    while new_lst1 != 0:
        connection5 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor1 = connection5.cursor()
        rows1=cursor1.execute("exec Generate_RDScheduler_ScheduleJobs" )
        rows1 = cursor1.fetchall()
        row1
        for row1 in rows1:
            print(row1)
        DFAID,DFATID,Dateto=row1
        #DFATID = ','.join(str(v) for v in DFATID)
        DFATID1=str(DFATID)
        DFAID1=str(DFAID)
        dateto=ProcDayStart1
        connection5.execute("delete from rpt_CurrentBalanceTable_SCH where InstitutionID in (%s) " % DFAID1)
        connection5.execute("delete from RPT_StatusTable_SP_sch where InstitutionID in (%s) " % DFAID1)
        connection5.execute("delete from RPT_ProductTransfer_Sch where InstitutionID in (%s) " % DFAID1)
        connection5.execute("delete from RPT_ProductTransfer_ChildLess_Sch where InstitutionID in (%s) " % DFAID1)
        connection5.execute("delete from RPT_ActiveCloseBranches_ProgramCount_SCH where InstitutionID in (%s) " % DFAID1)
        connection5.commit()
        print('delete')
        #mutex.acquire()
        th = Thread(target=current)
        th1=Thread(target=status)
        th2 = Thread(target=ProductTransfer)
        th3=Thread(target=ProductTransfer_ChildLess)
        th4=Thread(target=ActiveCloseBranches)
        th.start()
        print('thread1')
        th1.start()
        print('thread2')
        th2.start()
        print('thread3')
        th3.start()
        print('thread4')
        th4.start()
        
        print('thread5')
        #mutex.release()
        th.join()
        th1.join()
        th2.join()
        th3.join()
        th4.join()
        connection_count = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor_count = connection_count.cursor()
        sql = """select count(*) as CurrentBalance_count from rpt_CurrentBalanceTable_SCH where InstitutionID = ?"""
        params = (DFAID1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst1 = ','.join(str(v) for v in row1)
        sql = """select count(*) as StatusTable_count from RPT_StatusTable_SP_sch where InstitutionID = ?"""
        params = (DFAID1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst2 = ','.join(str(v) for v in row1)
        sql = """select count(*) as RPT_ProductTransfer_Sch_count from RPT_ProductTransfer_Sch where InstitutionID = ?"""
        params = (DFAID1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst3 = ','.join(str(v) for v in row1)
        sql = """select count(*) as RPT_ProductTransfer_ChildLess_Sch_count from RPT_ProductTransfer_ChildLess_Sch where InstitutionID = ?"""
        params = (DFAID1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst4 = ','.join(str(v) for v in row1)
        sql = """select count(*) as RPT_ActiveCloseBranches_ProgramCount_SCH from RPT_ActiveCloseBranches_ProgramCount_SCH where InstitutionID = ?"""
        params = (DFAID1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst5 = ','.join(str(v) for v in row1)        
        print('final')
        logger.info("-------------------------------------------------------------------------------------------------------------------")
        logger.info("Instituion =  "+DFAID1)
        logger.info("Total Number of Record :- " +new_lst1 +" Inserted into CurrentBalance balance EOD Table for SETTLEMENTCUTOFF")
        logger.info("Total Number of Record :- " +new_lst2 +" Inserted into Status EOD Table for SETTLEMENTCUTOFF")
        logger.info("Total Number of Record :- " +new_lst3 +" Inserted into RPT_ProductTransfer_Sch EOD Table for SETTLEMENTCUTOFF")
        #logger.info("Data insert Successfully for ProductTransfer_ChildLess EOD Table for " +DFAID1 +" Institution :Total Record count insert:- " +new_lst1)
        logger.info("Total Number of Record :- " +new_lst4 +" Inserted into ProductTransfer_ChildLess EOD Table for SETTLEMENTCUTOFF")
        logger.info("Total Number of Record :- " +new_lst4 +" Inserted into ActiveCloseBranches_ProgramCount EOD Table for SETTLEMENTCUTOFF")
        #logger.info("Recored Inserted Successfully into ActiveCloseBranches_ProgramCount EOD Table")
        connection_count.commit()
        ScheduleType='SETTLEMENTCUTOFF'
        connection6 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor2 = connection6.cursor()
        sql = """update  RDScheduler_ReportSchedules set NextDateTime=NextDateTime+1 where  aid=? and ScheduleType=?"""
        params = (DFAID1,ScheduleType)
        rows2= cursor2.execute(sql, params) 
        connection6.commit() 
        sql = """exec Generate_RDScheduler_RemaningJobCount"""
        #params = (DFATID1,DFAID1,reportdate)
        rows1= cursor.execute(sql)
        rows1 = cursor.fetchall()
        row1= ()
        for row1 in rows1:
            row1
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst1 = ','.join(str(v) for v in row1)
        new_lst1=int(new_lst1)   
        #logger.info('File Generated successfully')
else:
    logger.info('Need to Check Procdayend / DW Timming / and Report Scheduler supporting tables rdscheduler_reportschedules For BUSINESSDATE')           



# In[20]:


rows5=cursor.execute("select convert(varchar(23), cast((ProcDayend-1) as datetime), 121) as ProcDayend from arsystemaccounts" )
rows5 = cursor.fetchall()
row5= ()
for row5 in rows5:
    row5

new_lst5=('('.join(row5))


sql = """exec Generate_RDScheduler_businessCutOff_RemaningJobCount"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)
rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    row1
#new_lst2=('('.join(row1))
#new_lst1 
new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)


# In[553]:


def current1():
        connection_current1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_current1.cursor()
        sql = """        EXEC GenerateCurrentBalanceAsOnDate_businessCutOff_opttables_SCH @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_current1.commit()  
        #connection1.commit()


# In[554]:


def status1():
        connection_status1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #onnection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_status1.cursor()
        sql = """        EXEC GenerateStatusAsOnDate_businessCutOff_OptTables_withdate_sch @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_status1.commit()  
        #connection1.commit()


# In[555]:


def ProductTransfer1():
        connection_ProductTransfer1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_ProductTransfer1.cursor()
        sql = """    EXEC GenerateProductTransferLinks_businessCutOff_SH @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_ProductTransfer1.commit()  
        #connection1.commit()


# In[556]:


def ProductTransfer_ChildLess1():
        connection_ProductTransfer_ChildLess1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_ProductTransfer_ChildLess1.cursor()
        sql = """    EXEC GenerateBaseAccountsFrom_businessCutOff_XREF_PT @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_ProductTransfer_ChildLess1.commit()  
        #connection1.commit()


# In[557]:


# In[21]:


if ProcDayStart <= DW and new_lst1 !=0:
    while new_lst1 != 0:
        connection5 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor1 = connection5.cursor()
        rows1=cursor1.execute("exec Generate_RDScheduler_businessCutOff_ScheduleJobs" )
        rows1 = cursor1.fetchall()
        row1
        for row1 in rows1:
            print(row1)
        DFAID,DFATID,Dateto=row1
        #DFATID = ','.join(str(v) for v in DFATID)
        DFATID1=str(DFATID)
        DFAID1=str(DFAID)
        dateto1=new_lst5
        connection5.commit()
        th = Thread(target=current1)
        th1=Thread(target=status1)
        th2 = Thread(target=ProductTransfer1)
        th3=Thread(target=ProductTransfer_ChildLess1)
        #th4=Thread(target=ActiveCloseBranches)
        th.start()
        th1.start()
        th2.start()
        th3.start()
        #th4.start()
        
        print('thread')
        th.join()
        th1.join()
        th2.join()
        th3.join()
        #time.sleep(60)
        #print('thread')
        connection_count = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor_count = connection_count.cursor()
        sql = """select count(*) as CurrentBalance_count from rpt_CurrentBalanceTable_SCH where InstitutionID = ? and reportdate=?"""
        params = (DFAID1,dateto1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst1 = ','.join(str(v) for v in row1)
        sql = """select count(*) as StatusTable_count from RPT_StatusTable_SP_sch where InstitutionID = ? and reportdate=?"""
        params = (DFAID1,dateto1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst2 = ','.join(str(v) for v in row1)
        sql = """select count(*) as RPT_ProductTransfer_Sch_count from RPT_ProductTransfer_Sch where InstitutionID = ? and DateTo=?"""
        params = (DFAID1,dateto1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst3 = ','.join(str(v) for v in row1)
        sql = """select count(*) as RPT_ProductTransfer_ChildLess_Sch_count from RPT_ProductTransfer_ChildLess_Sch where InstitutionID = ?  and DateTo=?"""
        params = (DFAID1,dateto1)
        rows1= cursor_count.execute(sql,params)
        rows1 = cursor_count.fetchall()
        row1= ()
        for row1 in rows1:
            print(row1)
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst4 = ','.join(str(v) for v in row1)
        print('final')
        logger.info("-------------------------------------------------------------------------------------------------------------------")
        logger.info("Instituion =  "+DFAID1)
        logger.info("Total Number of Record :- " +new_lst1 +" Inserted into CurrentBalance balance EOD Table for BUSINESSDATE")
        logger.info("Total Number of Record :- " +new_lst2 +" Inserted into Status EOD Table for BUSINESSDATE")
        logger.info("Total Number of Record :- " +new_lst3 +" Inserted into RPT_ProductTransfer_Sch EOD Table for BUSINESSDATE")
        #logger.info("Data insert Successfully for ProductTransfer_ChildLess EOD Table for " +DFAID1 +" Institution :Total Record count insert:- " +new_lst1)
        logger.info("Total Number of Record :- " +new_lst4 +" Inserted into ProductTransfer_ChildLess EOD Table for BUSINESSDATE")
        #logger.info("Total Number of Record :- " +new_lst4 +" Inserted into ActiveCloseBranches_ProgramCount EOD Table for SETTLEMENTCUTOFF")
        #logger.info("Recored Inserted Successfully into ActiveCloseBranches_ProgramCount EOD Table")
        connection_count.commit()
        
        ScheduleType='BUSINESSDATE'
        connection6 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor2 = connection6.cursor()
        sql = """update  RDScheduler_ReportSchedules set NextDateTime=NextDateTime+1 where  aid=? and ScheduleType=?"""
        params = (DFAID1,ScheduleType)
        rows2= cursor2.execute(sql, params)
        connection6.commit() 
        sql = """exec Generate_RDScheduler_businessCutOff_RemaningJobCount"""
        #params = (DFATID1,DFAID1,reportdate)
        rows1= cursor.execute(sql)
        rows1 = cursor.fetchall()
        row1= ()
        for row1 in rows1:
            row1
        #new_lst2=('('.join(row1))
        #new_lst1 
        new_lst1 = ','.join(str(v) for v in row1)
        new_lst1=int(new_lst1)   
        #logger.info('File Generated successfully')
else:
    logger.info('Need to Check Procdayend / DW Timming / and Report Scheduler supporting tables rdscheduler_reportschedules For SETTLEMENTCUTOFF')           


# In[22]:


rows5=cursor.execute("select convert(varchar(23), cast((ProcDayend-1) as datetime), 121) as ProcDayend from arsystemaccounts" )
rows5 = cursor.fetchall()
row5= ()
for row5 in rows5:
    row5

new_lst5=('('.join(row5))



# In[23]:


sql = """exec Generate_RDScheduler_businessCutOff_Billing_RemaningJobCount"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)
rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    row1
#new_lst2=('('.join(row1))
#new_lst1 
new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)


# In[553]:



# In[24]:





# In[ ]:


def current1():
        connection_current1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #connection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_current1.cursor()
        sql = """        EXEC GenerateCardAccountFileFeeActiveInactive_FeeTable_HL_SH @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_current1.commit()  
        #connection1.commit()


# In[554]:


# In[ ]:



def status1():
        connection_status1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        #onnection1 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=databaseDW(),UID=UID(),PWD=PWD(), trusted_connection='Yes')
        cursor = connection_status1.cursor()
        sql = """        EXEC GenerateStatusAsOnDate_businessCutOff_OptTables_withdate_sch @InstitutionID = ?, @ProductID = ?, @ReportDate = ?
        """
        params = (DFAID1,0,dateto1)
        cursor.execute(sql, params)
        connection_status1.commit()  
        #connection1.commit()


# In[54]:


connection2 = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=server(), database=database(),UID=UID(),PWD=PWD(), trusted_connection='Yes')


# In[55]:


cursor1 = connection2.cursor()
rows1=cursor1.execute("select rs.atid,rs.aid,rs.ScheduleType from rdscheduler_reportschedules rs join arsystemaccounts ar on rs.nextdatetime<>ar.ProcDayEnd" )
rows1 = cursor1.fetchall()


# In[56]:


logger.info("Need to check EOD Table utility log as Data not Inserted for Institution :-"  )
logger.info(rows1)
connection2.commit()  
#connection1.commit() 


# In[ ]:





# In[ ]:





# In[57]:


SERVER = SMTPSERVER()
FROM = FROM()
TO = list(TO().split(",")) # must be a list

SUBJECT = "[CPP]-[Prod]- RD-EOD Tables Data Filling Log -"+ProcDayend
#TEXT = "Manager"


# In[58]:


filename1


# In[59]:


filepath=filename1
with open(filepath, 'r') as textfile:
    content = textfile.readlines()
    email_body = ''.join(content) # new line characters will be included
    textfile.close()


# In[60]:


message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n
%s
""" % (FROM, ", ".join(TO), SUBJECT, email_body)

# Send the mail


# In[61]:


TO


# In[62]:


import smtplib
server = smtplib.SMTP(SERVER)
server.sendmail(FROM, TO, message)
server.quit()


# In[63]:


connection.commit()  
connection1.commit()  

logger.info("-------------------------------------------------------------------------------------------------------------------")

logging.shutdown()

