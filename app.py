from flask import Flask, request, render_template, make_response
import pandas as pd
import datetime
import requests
import math
import sys
import base64
import flask_excel as excel


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
def jltodate(x):
    x = str(x)
    year = str(((int(x[0]) + 19)*100) + int((x[1:3])))
    date = '01/01/' + year
    date_time_obj = datetime.datetime.strptime(date, '%m/%d/%Y').date()
    end_date = date_time_obj + datetime.timedelta(days=(int(x)%1000) -1)
    return end_date
env=''
auth=''
def env1(x1):
    if x1 == '1':
        env = '-dev1'
    elif x1 == '2':
        env = '-dev2'
    elif x1 == '3':
        env = '-dev3'
    elif x1 == '4':
        env = '-test'
    elif x1 == '5':
        env = ''
    else:
        render_template('form_ex.html', error='Choose correct value')
    return env

def auth1(uname,psw):
    ass = request.form['uname'] + ':' + request.form['psw']
    sample_string_bytes = ass.encode("ascii") 
    base64_bytes = base64.b64encode(sample_string_bytes) 
    auth = base64_bytes.decode("ascii") 
    return auth


payload={}



app = Flask(__name__)
@app.route('/')
def my_form():
    return render_template('form_ex.html')


@app.route('/home')
@app.route('/', methods = ['POST'])

def my_form_post():
    if request.method == 'POST':
        x1 = request.form['Env']
        env = env1(x1)
        url = "https://efmr{0}.fa.us6.oraclecloud.com/fscmRestApi/resources/latest/ContractsUnitRelationship_c".format(env)
        uurl = "https://efmr{0}.fa.us6.oraclecloud.com/fscmRestApi/resources/latest/UnitMaster_c".format(env)
        auth = auth1(request.form['uname'],request.form['psw'])
        headers = {
      'Content-Type': 'application/vnd.oracle.adf.resourceitem+json',
      'REST-Framework-Version': '4',
      'Authorization': 'Basic {0}'.format(auth)
        }
        if request.form['submit'] == 'Submit_F0006':
            file = request.files['F0006_csv']
            dff0006 = pd.read_excel(file,dtype = str)
            df_F0006_coun = df_conunit.copy()
            sr=0
            while sr < len(dff0006.columns):
                dff0006[dff0006.columns[sr]] = dff0006[dff0006.columns[sr]].str.strip()
                sr = sr+1
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
            if len(df)>0:

                c7 = df_F0006_coun.columns.get_loc("RecordNumber")
                df_F0006_coun.drop(df_F0006_coun.columns[[c7]], axis = 1, inplace = True) 
    
                all_F0006_coun = pd.merge(df_F0006_coun,df,on = 'RecordName',how = 'left')
            else:
                all_F0006_coun = df_F0006_coun.copy()
                #all_F0006_coun.to_csv(file, index = False)
            print(all_F0006_coun.head())
            
           
            resp = make_response(all_F0006_coun.to_csv(index = False))
            resp.headers["Content-Disposition"] = "attachment; filename=F0006.csv"
            resp.headers["Content-Type"] = "text/csv"
            
        return resp
        

if __name__ == '__main__':
    app.run('localhost',5050)


