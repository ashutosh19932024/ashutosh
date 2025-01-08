#!/usr/bin/env python
# coding: utf-8

# In[34]:


import pandas as pd
#import pandas.io.sql as psql
import pyodbc
import json
import warnings
#import config as cn
warnings.filterwarnings('ignore')
import subprocess
import logging
import time
import shutil
import os
import datetime


# In[35]:


def logpath():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    logpath=config['logpath']
    #print(mongodb)
    return(logpath)
print(logpath())


# In[36]:


# datetime.datetime.now() to get
# current date as filename.

filename = datetime.datetime.now()
f=filename.strftime('%d %B %Y')
f='schedule_log'+f
#f
path=logpath()
filename1=path+f+'.log'
#logging.basicConfig(filename=filename1, level=logger.info)


# In[37]:


logger = logging.getLogger("OSA")

logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(filename1, mode="a")#'a' for append you can use 'w' for write

formatter = logging.Formatter("%(asctime)s:  %(message)s",
                              "%Y-%m-%d %H:%M:%S")
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)


# In[38]:


logger.info("-------------------------------------------------------------------------------------------------------------------")


# In[39]:


def AuthenticationMode():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    AuthenticationMode=config['AuthenticationMode']
    #print(mongodb)
    return(AuthenticationMode)
logger.info(AuthenticationMode())


# In[40]:


def DFDBSERVER():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFDBSERVER=config['DFDBSERVER']
    #print(mongodb)
    return(DFDBSERVER)
logger.info(DFDBSERVER())


# In[41]:


def DFDBCOREISSUE():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFDBCOREISSUE=config['DFDBCOREISSUE']
    #print(mongodb)
    return(DFDBCOREISSUE)
logger.info(DFDBCOREISSUE())


# In[6]:


def DFDBUSERNAME():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFDBUSERNAME=config['DFDBUSERNAME']
    #print(mongodb)
    return(DFDBUSERNAME)
logger.info(DFDBUSERNAME())


# In[7]:


def DFDBPASSWORD():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFDBPASSWORD=config['DFDBPASSWORD']
    #print(mongodb)
    return(DFDBPASSWORD)
logger.info(DFDBPASSWORD())


# In[8]:




# In[11]:


def PathName():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    PathName=config['PathName']
    #print(mongodb)
    return(PathName)
logger.info(PathName())


# In[12]:


def ErrorFolderMeta():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    ErrorFolderMeta=config['ErrorFolderMeta']
    #print(mongodb)
    return(ErrorFolderMeta)
logger.info(ErrorFolderMeta())


# In[67]:


def LogFileFolderName():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    LogFileFolderName=config['LogFileFolderName']
    #print(mongodb)
    return(LogFileFolderName)
logger.info(LogFileFolderName())


# In[14]:


def DFOUTPUTFILENAME():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFOUTPUTFILENAME=config['DFOUTPUTFILENAME']
    #print(mongodb)
    return(DFOUTPUTFILENAME)
logger.info(DFOUTPUTFILENAME())


# In[15]:


def emKMSdefaultMachines():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    emKMSdefaultMachines=config['emKMSdefaultMachines']
    #print(mongodb)
    return(emKMSdefaultMachines)
logger.info(emKMSdefaultMachines())


# In[16]:


def NetworkDrive():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    NetworkDrive=config['NetworkDrive']
    #print(mongodb)
    return(NetworkDrive)
logger.info(NetworkDrive())


# In[17]:


def BinariesDrive():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    BinariesDrive=config['BinariesDrive']
    #print(mongodb)
    return(BinariesDrive)
logger.info(BinariesDrive())


# In[18]:


def Filetype():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    Filetype=config['Filetype']
    #print(mongodb)
    return(Filetype)
logger.info(Filetype())


# In[19]:


def DFClientFlag():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFClientFlag=config['DFClientFlag']
    #print(mongodb)
    return(DFClientFlag)
logger.info(DFClientFlag())


# In[20]:


def DFIsManual():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    DFIsManual=config['DFIsManual']
    #print(mongodb)
    return(DFIsManual)
logger.info(DFIsManual())


# In[21]:


def databaseDW():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    databaseDW=config['databaseDW']
    #print(mongodb)
    return(databaseDW)
logger.info(databaseDW())


# In[22]:


def BatchFile_path():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    BatchFile_path=config['BatchFile_path']
    #print(mongodb)
    return(BatchFile_path)
logger.info(BatchFile_path())


# In[23]:





def driver():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    driver=config['driver']
    #print(mongodb)
    return(driver)
logger.info(driver())


# In[42]:



def Target_logfile():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    Target_logfile=config['Target_logfile']
    #print(mongodb)
    return(Target_logfile)
logger.info(Target_logfile())


# In[43]:



def Target_Outputfile():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    Target_Outputfile=config['Target_Outputfile']
    #print(mongodb)
    return(Target_Outputfile)
logger.info(Target_Outputfile())


# In[44]:



def SleepinSec():
    filename="config\\config.config"
    contents=open(filename).read()
    config=eval(contents)
    SleepinSec=config['SleepinSec']
    #print(mongodb)
    return(SleepinSec)
logger.info(SleepinSec())


# In[45]:


connection = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
connection1 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
cursor = connection.cursor()


# In[46]:


logger.info(connection)
logger.info(connection1)
logger.info(cursor)


# In[47]:



rows=cursor.execute("select CONVERT(VARCHAR(10),ProcDayStart,101) as ProcDayStart from arsystemaccounts" )
rows = cursor.fetchall()
row= ()
for row in rows:
    logger.info(row)


# In[48]:


new_lst4=('('.join(row))
new_lst= datetime.datetime.strptime(new_lst4, "%m/%d/%Y").strftime("%Y-%m-%d")
#print(x)


# In[49]:


#new_lst


# In[50]:


SleepinSec=int(SleepinSec())


# In[51]:


sql = """exec GenerateDF_File_Generator_Job_count"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)


# In[52]:


rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    logger.info(row1)
#new_lst2=('('.join(row1))
#new_lst1 
connection.commit()  
connection1.commit()  


# In[53]:


new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)


# In[54]:


BatchFile_path=BatchFile_path()


# In[55]:


if new_lst1 !=0:
    while new_lst1 != 0:
        connection_loo1 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        connection_DW_loop1 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        cursor = connection_loo1.cursor()
        rows1=cursor.execute("exec GenerateDF_File_Generator_Job" )
        rows1 = cursor.fetchall()
        row1
        for row1 in rows1:
            logger.info(row1)
        DFAID,DFATID,batchname,Dateto=row1
        #DFATID = ','.join(str(v) for v in DFATID)
        DFATID1=str(DFATID)
        DFAID1=str(DFAID)
        #rows=cursor.execute("select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?" )
        sql = """select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?"""
        params = (DFAID)
        rows3= cursor.execute(sql, params)
        #rows = cursor.fetchall()
        row= ()
        for row in rows3:
            logger.info(row)
        Dateto1=('('.join(row))    
        Dateto2=Dateto.strftime('%d-%m-%Y %H:%M:%S')
        #Dateto1=str(Dateto1)
        #Dateto1=('('.join(Dateto1))
        filepath1=DFOUTPUTFILENAME()+DFATID1+'-'+DFAID1+'\\'+'Reports'+'\\'+new_lst+'\\'+'CoreIssue'+'\\'+DFATID1+'-'+DFAID1+'\\'
        logger.info(filepath1)
        n=new_lst
        n1=n.replace('-', '')
        # checking if the directory demo_folder2
        # exist or not.
        if not os.path.isdir(filepath1):
            os.makedirs(filepath1)
        f1='CustomerMasterFile'+'_'+DFATID1+'_'+DFAID1+'_'+n1
        x=''    
        path=filepath1
        filename1=path+f1+'.txt'
        # create empty file
        file_name = filename1
        if os.path.isfile(file_name):
            expand = 1
            while True:
                expand += 1
                new_file_name = file_name.split(".txt")[0]+'_00' + str(expand) + ".txt"
                if os.path.isfile(new_file_name):
                    continue
                else:
                    file_name = new_file_name
                    break
        def create_file():
        # Function creates an empty file
        # %d - date, %B - month, %Y - Year
            with open(file_name, "w") as file:
                file.write(x)
        # Driver Code
        create_file() 
        print(file_name)
        BatchFile_path=BatchFile_path
        batch=BatchFile_path+batchname
        subprocess.call([batch,DFDBSERVER(),DFDBCOREISSUE(),DFDBUSERNAME(),DFDBPASSWORD(),DFATID1,DFAID1,Dateto1,PathName(),ErrorFolderMeta(),LogFileFolderName(),file_name,emKMSdefaultMachines(),NetworkDrive(),BinariesDrive(),Filetype(),DFClientFlag(),DFIsManual()])
        #shutil.move(original, target)
        #file_names = os.listdir(source_dir)
        #for file_name in file_names:
         #   shutil.move(os.path.join(source_dir, file_name), target_dir)
        sql = """update  RPT_DF_File_Generator_schedule_table set NextDateTime=NextDateTime+1 where  aid=?"""
        params = (DFAID)
        rows2= cursor.execute(sql, params) 
        sql = """exec GenerateDF_File_Generator_Job_count"""
        #params = (DFATID1,DFAID1,reportdate)
        rows1= cursor.execute(sql)
        rows1 = cursor.fetchall()
        row1= ()
        for row1 in rows1:
            logger.info(row1)
        new_lst1 = ','.join(str(v) for v in row1)
        new_lst1=int(new_lst1)    
        logger.info('File Generated successfully')
        connection_loo1.commit()  
        connection_DW_loop1.commit() 
else:
    logger.info('RPT table data Not available')
    time.sleep(SleepinSec)    


# In[56]:


connection1 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
connection2 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
cursor = connection1.cursor()
sql = """exec GenerateDF_File_Generator_Job_count"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)


# In[57]:


rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    logger.info(row1)
#new_lst2=('('.join(row1))
#new_lst1 


# In[58]:


new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)
connection1.commit()  
connection2.commit()  


# In[59]:


if new_lst1 !=0:
    while new_lst1 != 0:
        connection_loo2 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        connection_DW_loop2 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        cursor = connection_loo2.cursor()
        rows1=cursor.execute("exec GenerateDF_File_Generator_Job" )
        rows1 = cursor.fetchall()
        row1
        for row1 in rows1:
            logger.info(row1)
        DFAID,DFATID,batchname,Dateto=row1
        #DFATID = ','.join(str(v) for v in DFATID)
        DFATID1=str(DFATID)
        DFAID1=str(DFAID)
        #rows=cursor.execute("select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?" )
        sql = """select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?"""
        params = (DFAID)
        rows3= cursor.execute(sql, params)
        #rows = cursor.fetchall()
        row= ()
        for row in rows3:
            logger.info(row)
        Dateto1=('('.join(row))    
        Dateto2=Dateto.strftime('%d-%m-%Y %H:%M:%S')
        #Dateto1=str(Dateto1)
        #Dateto1=('('.join(Dateto1))
        filepath1=DFOUTPUTFILENAME()+DFATID1+'-'+DFAID1+'\\'+'Reports'+'\\'+new_lst+'\\'+'CoreIssue'+'\\'+DFATID1+'-'+DFAID1+'\\'
        logger.info(filepath1)
        n=new_lst
        n1=n.replace('-', '')
        # checking if the directory demo_folder2
        # exist or not.
        if not os.path.isdir(filepath1):
            os.makedirs(filepath1)
        f1='CustomerMasterFile'+'_'+DFATID1+'_'+DFAID1+'_'+n1
        x=''    
        path=filepath1
        filename1=path+f1+'.txt'
        # create empty file
        file_name = filename1
        if os.path.isfile(file_name):
            expand = 1
            while True:
                expand += 1
                new_file_name = file_name.split(".txt")[0]+'_00' + str(expand) + ".txt"
                if os.path.isfile(new_file_name):
                    continue
                else:
                    file_name = new_file_name
                    break
        def create_file():
        # Function creates an empty file
        # %d - date, %B - month, %Y - Year
            with open(file_name, "w") as file:
                file.write(x)
        # Driver Code
        create_file() 
        print(file_name)
        BatchFile_path=BatchFile_path
        batch=BatchFile_path+batchname
        subprocess.call([batch,DFDBSERVER(),DFDBCOREISSUE(),DFDBUSERNAME(),DFDBPASSWORD(),DFATID1,DFAID1,Dateto1,PathName(),ErrorFolderMeta(),LogFileFolderName(),file_name,emKMSdefaultMachines(),NetworkDrive(),BinariesDrive(),Filetype(),DFClientFlag(),DFIsManual()])
        #shutil.move(original, target)
        #file_names = os.listdir(source_dir)
        #for file_name in file_names:
         #   shutil.move(os.path.join(source_dir, file_name), target_dir)
        sql = """update  RPT_DF_File_Generator_schedule_table set NextDateTime=NextDateTime+1 where  aid=?"""
        params = (DFAID)
        rows2= cursor.execute(sql, params) 
        sql = """exec GenerateDF_File_Generator_Job_count"""
        #params = (DFATID1,DFAID1,reportdate)
        rows1= cursor.execute(sql)
        rows1 = cursor.fetchall()
        row1= ()
        for row1 in rows1:
            logger.info(row1)
        new_lst1 = ','.join(str(v) for v in row1)
        new_lst1=int(new_lst1)    
        logger.info('File Generated successfully')
        connection_loo2.commit()  
        connection_DW_loop2.commit() 
else:
    logger.info('RPT table data Not available')
    time.sleep(SleepinSec)    


# In[60]:


connection2 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
connection3 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
cursor = connection2.cursor()
sql = """exec GenerateDF_File_Generator_Job_count"""
#params = (DFATID1,DFAID1,reportdate)
rows1= cursor.execute(sql)


# In[61]:


rows1 = cursor.fetchall()
row1= ()
for row1 in rows1:
    logger.info(row1)
#new_lst2=('('.join(row1))
#new_lst1 


# In[62]:


new_lst1 = ','.join(str(v) for v in row1)
new_lst1=int(new_lst1)
connection2.commit()  
connection3.commit() 


# In[63]:


if new_lst1 !=0:
    while new_lst1 != 0:
        connection_loo3 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=DFDBCOREISSUE(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        connection_DW_loop3 = pyodbc.connect(driver='{SQL Server}', server=DFDBSERVER(), database=databaseDW(),UID=DFDBUSERNAME(),PWD=DFDBPASSWORD(), trusted_connection='NO')
        cursor = connection_loo3.cursor()
        rows1=cursor.execute("exec GenerateDF_File_Generator_Job" )
        rows1 = cursor.fetchall()
        row1
        for row1 in rows1:
            logger.info(row1)
        DFAID,DFATID,batchname,Dateto=row1
        #DFATID = ','.join(str(v) for v in DFATID)
        DFATID1=str(DFATID)
        DFAID1=str(DFAID)
        #rows=cursor.execute("select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?" )
        sql = """select CONVERT(VARCHAR(10),NextDateTime,101) as NextDateTime from RPT_DF_File_Generator_schedule_table where aid=?"""
        params = (DFAID)
        rows3= cursor.execute(sql, params)
        #rows = cursor.fetchall()
        row= ()
        for row in rows3:
            logger.info(row)
        Dateto1=('('.join(row))    
        Dateto2=Dateto.strftime('%d-%m-%Y %H:%M:%S')
        #Dateto1=str(Dateto1)
        #Dateto1=('('.join(Dateto1))
        filepath1=DFOUTPUTFILENAME()+DFATID1+'-'+DFAID1+'\\'+'Reports'+'\\'+new_lst+'\\'+'CoreIssue'+'\\'+DFATID1+'-'+DFAID1+'\\'
        logger.info(filepath1)
        n=new_lst
        n1=n.replace('-', '')
        # checking if the directory demo_folder2
        # exist or not.
        if not os.path.isdir(filepath1):
            os.makedirs(filepath1)
        f1='CustomerMasterFile'+'_'+DFATID1+'_'+DFAID1+'_'+n1
        x=''    
        path=filepath1
        filename1=path+f1+'.txt'
        # create empty file
        file_name = filename1
        if os.path.isfile(file_name):
            expand = 1
            while True:
                expand += 1
                new_file_name = file_name.split(".txt")[0]+'_00' + str(expand) + ".txt"
                if os.path.isfile(new_file_name):
                    continue
                else:
                    file_name = new_file_name
                    break
        def create_file():
        # Function creates an empty file
        # %d - date, %B - month, %Y - Year
            with open(file_name, "w") as file:
                file.write(x)
        # Driver Code
        create_file() 
        print(file_name)
        BatchFile_path=BatchFile_path
        batch=BatchFile_path+batchname
        subprocess.call([batch,DFDBSERVER(),DFDBCOREISSUE(),DFDBUSERNAME(),DFDBPASSWORD(),DFATID1,DFAID1,Dateto1,PathName(),ErrorFolderMeta(),LogFileFolderName(),file_name,emKMSdefaultMachines(),NetworkDrive(),BinariesDrive(),Filetype(),DFClientFlag(),DFIsManual()])
        #shutil.move(original, target)
        #file_names = os.listdir(source_dir)
        #for file_name in file_names:
         #   shutil.move(os.path.join(source_dir, file_name), target_dir)
        sql = """update  RPT_DF_File_Generator_schedule_table set NextDateTime=NextDateTime+1 where  aid=?"""
        params = (DFAID)
        rows2= cursor.execute(sql, params) 
        sql = """exec GenerateDF_File_Generator_Job_count"""
        #params = (DFATID1,DFAID1,reportdate)
        rows1= cursor.execute(sql)
        rows1 = cursor.fetchall()
        row1= ()
        for row1 in rows1:
            logger.info(row1)
        new_lst1 = ','.join(str(v) for v in row1)
        new_lst1=int(new_lst1)    
        logger.info('File Generated successfully')
        connection_loo3.commit()  
        connection_DW_loop3.commit() 
else:
    logger.info('RPT table data Not available')
    #time.sleep(SleepinSec)    


# In[64]:


#connection.commit()  
#connection1.commit()  


# In[65]:


logging.shutdown()


# In[ ]:




