#!/usr/bin/env python
# coding: utf-8

# In[36]:


import pandas as pd
import datetime
import requests
import math
import sys
import base64


# In[37]:


cenv = input("""Enter Env: \n
A) dev1 - 1
B) dev2 - 2
C) dev3 - 3
D) TEST - 4
E) Prod - 100\n""")

if cenv == '1':
    env = '-dev1'
elif cenv == '2':
    env = '-dev2'
elif cenv == '3':
    env = '-dev3'
elif cenv == '4':
    env = '-test'
elif cenv == '100':
    env = ''
else:
    print("Please enter correct input")
    sys.exit()
file_loc = input("Enter file location for F0006 excel:")
file_output = input("Enter file location for output file csv:")
username = input("Enter username for efmr{0}:".format(env))
password = input("Enter password for efmr{0}:".format(env))
url = "https://efmr{0}.fa.us6.oraclecloud.com/fscmRestApi/resources/latest/ContractsUnitRelationship_c".format(env)
uurl = "https://efmr{0}.fa.us6.oraclecloud.com/fscmRestApi/resources/latest/UnitMaster_c".format(env)
print(url)
print(uurl)

ass = username + ':' + password
sample_string_bytes = ass.encode("ascii") 
base64_bytes = base64.b64encode(sample_string_bytes) 
auth = base64_bytes.decode("ascii") 

headers = {
      'Content-Type': 'application/vnd.oracle.adf.resourceitem+json',
      'REST-Framework-Version': '4',
      'Authorization': 'Basic {0}'.format(auth)
    }

payload={}
#print(file_output)


# In[38]:


df_conunit = pd.DataFrame(columns=['Id',
'RecordName',
'CreatedBy',
'CreationDate',
'ConflictId',
'ContractNumber_c',
'UnitNumber_c',
'ContractDescription_c',
'BusinessUnit_c',
'Flag_c',
'Active_c',
'JDECompany_c',
'RecordNumber',
'UserLastUpdateDate',
'CurrencyCode',
'CurcyConvRateType',
'CorpCurrencyCode',
'ContractType_c',
'ContractEffectiveDate_c',
'ContractCompletionDate_c',
'Supervisor_c',
'SelectedRow',
'ContractStartDate_c',
'BuildingID_c',
'Status_c'                                
])

df_unit = pd.DataFrame(columns = ['Id',
'RecordName',
'ConflictId',
'JDECompany_c',
'BusinessUnit_c',
'UnitNumber_c',
'Active_c',
'RecordNumber',
'CurrencyCode',
'Status_c'
])
df_unit.head()


# In[39]:


dff0006 = pd.read_excel(file_loc,dtype = str)
df_F0006_coun = df_conunit.copy()
sr=0
while sr < len(dff0006.columns):
    dff0006[dff0006.columns[sr]] = dff0006[dff0006.columns[sr]].str.strip()
    sr = sr+1

#dff0006['MCCO'] = '00'+ dff0006['MCCO']


def jltodate(x):
    x = str(x)
    year = str(((int(x[0]) + 19)*100) + int((x[1:3])))
    date = '01/01/' + year
    date_time_obj = datetime.datetime.strptime(date, '%m/%d/%Y').date()
    end_date = date_time_obj + datetime.timedelta(days=(int(x)%1000) -1)
    return end_date
dff0006.head()


# In[40]:


a=0
c1 = dff0006.columns.get_loc("MCD4J")
c2 = dff0006.columns.get_loc("MCD1J")
while a < len(dff0006):
    try:
        
        if dff0006.iloc[a][c1] == '0':
            dff0006.iloc[a][c1] = ''
        else:
            dff0006.iloc[a][c1] = jltodate(dff0006.iloc[a][c1])
    except:
        dff0006.iloc[a][c1] = ''
        
    
    try:
        
        if dff0006.iloc[a][c2] == '0':
            dff0006.iloc[a][c2] = ''
        else:
            dff0006.iloc[a][c2] = jltodate(dff0006.iloc[a][c2])
    except:
         dff0006.iloc[a][c2] = ''

    a = a+1

df_F0006_coun['ContractNumber_c'] = dff0006['MCMCU']
df_F0006_coun['UnitNumber_c'] = '00000000'
df_F0006_coun['ContractDescription_c'] = dff0006['MCDL01']
df_F0006_coun['BusinessUnit_c'] = dff0006['MCRP23']
df_F0006_coun['Flag_c'] = 'JC'
#df_F0006_coun['Active_c'] = 'Y'
df_F0006_coun['JDECompany_c'] = dff0006['MCCO']
df_F0006_coun['ContractType_c'] = dff0006['MCSTYL']
df_F0006_coun['ContractCompletionDate_c'] = dff0006['MCD4J']
df_F0006_coun['Supervisor_c'] = dff0006['MCRP22']
df_F0006_coun['ContractStartDate_c'] = dff0006['MCD1J']
df_F0006_coun['BuildingID_c'] = dff0006['MCAN8']
df_F0006_coun['Status_c'] = dff0006['MCRP15']
df_F0006_coun['RecordName'] = dff0006['MCMCU'] + '_00000000'
df_F0006_coun['Active_c'] = df_F0006_coun['Status_c'].apply(lambda x: 'N' if x =='C' or x =='S' else 'Y')
#Find Duplicates
duplicateRowsDF = df_F0006_coun[df_F0006_coun.duplicated(['RecordName'], keep=False)]
#print(duplicateRowsDF)
print("Total Duplicate rows in F0006-Contract:",len(duplicateRowsDF.index))
#Drop Duplicates
df_F0006_coun.drop_duplicates(subset ="RecordName",keep = 'first', inplace = True)
print("Duplicates record deleted:")

del duplicateRowsDF

 # Create Unit Master From F0006
df_unitF0006 = df_unit.copy()
df_unitF0006['JDECompany_c'] = dff0006['MCCO']
df_unitF0006['BusinessUnit_c'] = dff0006['MCRP23']
df_unitF0006['UnitNumber_c'] = '00000000'
df_unitF0006['Active_c'] = 'Y'
df_unitF0006['Status_c'] = dff0006['MCRP15']
df_unitF0006['RecordName'] = '00000000_' + dff0006['MCCO']
#df_unitF0006.head()

#Find Duplicates
duplicateRowsDF = df_unitF0006[df_unitF0006.duplicated(['RecordName'], keep=False)]
print("Total Duplicate rows in F0006-Unit:",len(duplicateRowsDF.index))
df_unitF0006.drop_duplicates(subset ="RecordName",keep = 'first', inplace = True)
print("Duplicates record deleted:")

del duplicateRowsDF

print("Completed-Unit_F0006")


# In[41]:


#Update Mode COntracct
rcord = ""
x = len(df_F0006_coun)/200
x=math.ceil(x)
c6 = df_F0006_coun.columns.get_loc("RecordName")
a = 0
a1 = 0
a2 = 0
print("Connecting with Oracle-Contract Unit Relationship.Please Wait...")
while a < x:
    a1 = 0
    while a1<200:
        if a2<len(df_F0006_coun):
            rcord = rcord + "'" + str(df_F0006_coun.iloc[a2][c6]) + "',"
            a2 = a2+1
            a1 = a1 + 1
        else:
            break
    
    rcord = rcord[:-1]
    furl = url + "?q=RecordName in({0})&fields=RecordName,RecordNumber&onlyData=True&limit=250".format(rcord)
    #print(furl)
    
    response = requests.request("GET", furl, headers=headers, data = payload)

    #print(response.text.encode('utf8'))
    a_dic = response.json()
    b2 = a_dic['items']
    #print (b2)
    #print(type(b2))
    if a ==0:
        df = pd.DataFrame(b2)
    else:
        df = df.append(b2)
    rcord = ""
    a = a+1
print("Total records for update in Contract Unit Relationship:" + str(len(df)))
#df.head(10)


# In[42]:


# to be checked later
'''
c7 = df_F0006_coun.columns.get_loc("RecordNumber")
df_F0006_coun.drop(df_F0006_coun.columns[[c7]], axis = 1, inplace = True) 
#df_F55_coun.columns
all_F0006_coun = pd.merge(df_F0006_coun,df,on = 'RecordName',how = 'left')
file =  file_output + '\\F0006_contract.csv'
all_F0006_coun.to_csv(file, index = False)
print("Contract Unit Relationship Generated")
'''


# In[43]:


file =  file_output + '\\F0006_contract.csv'
if len(df)>0:
    c7 = df_F0006_coun.columns.get_loc("RecordNumber")
    df_F0006_coun.drop(df_F0006_coun.columns[[c7]], axis = 1, inplace = True) 
    #df_F55_coun.columns
    all_F0006_coun = pd.merge(df_F0006_coun,df,on = 'RecordName',how = 'left')
    #all_F0006_coun.to_csv(file, index = False)
else:
    all_F0006_coun = df_F0006_coun.copy()
all_F0006_coun.to_csv(file, index = False)

print("Contract Unit Relationship Generated")


# In[44]:


#all_F0006_coun.to_csv(file, index = False)


# In[45]:


'''
#Update Mode Unit
rcord = ""
x = len(df_unitF0006)/200
x=math.ceil(x)
c6 = df_unitF0006.columns.get_loc("RecordName")
a = 0
a1 = 0
a2 = 0
print("Connecting with Oracle-Unit Master.Please Wait...")
while a < x:
    a1 = 0
    while a1<200:
        if a2<len(df_unitF0006):
            rcord = rcord + "'" + str(df_unitF0006.iloc[a2][c6]) + "',"
            a2 = a2+1
            a1 = a1 + 1
        else:
            break
    
    rcord = rcord[:-1]
    aurl = uurl + "?q=RecordName in({0})&fields=RecordName,RecordNumber&onlyData=True&limit=250".format(rcord)
    #print(aurl)
    
    response = requests.request("GET", aurl, headers=headers, data = payload)

    #print(response.text.encode('utf8'))
    a1_dic = response.json()
    b3 = a1_dic['items']
    
    if a ==0:
        dfu = pd.DataFrame(b3)
    else:
        dfu = dfu.append(b3)
    rcord = ""
    a = a+1
print("Total records for update in Unit Master:" + str(len(dfu)))
#dfu.head(10)
'''


# In[46]:


'''
c7 = df_unitF0006.columns.get_loc("RecordNumber")
#print(c7)
df_unitF0006.drop(df_unitF0006.columns[[c7]], axis = 1, inplace = True) 
df_unitF0006 = pd.merge(df_unitF0006,dfu,on = 'RecordName',how = 'left')
file1 =  file_output + '\\F0006_unit.csv'
df_unitF0006.to_csv(file1, index = False)
print("Unit Master Generated")
'''

