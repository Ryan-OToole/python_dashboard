# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 07:54:36 2020

@author: tmazaherikou
"""

#Tahereh Mazaheri @ DTC_NOla

# This script reads location from the geoservice URL: should be run with Geoservice URl
import time
start = time.time()
def output():
    from time import sleep
    import pandas as pd
    import numpy as np
    import urllib.request
    import datetime
    import influxdb
    #client = influxdb.InfluxDBClient('localhost', 8086, database = 'dummydb')
    #q = "select * from abc1"
    #df = pd.DataFrame(client.query(q).get_points())
    
    df = pd.read_csv(r"C:\Users\tmazaherikou\Documents\abc.csv")
    
    df.rename(columns={'Current Cell ID': 'Current_Cell_ID', 'Procedure ID': 'ProcedureID'}, inplace=True)


    df["Alert"]=0.0
    df = df.fillna(0)
    print(type(df.time[0]))
    #df =pd.DataFrame(df, columns =['time','IMEI', 'IMSI','Previous IMSI']) 
    #df =pd.DataFrame(df)
    #print(df.head())
    df.set_index('time', inplace=True, drop=True)
    print(df.head())
    import datetime
    from datetime import datetime
    from datetime import date
    import json
    
    # Add a new column for time in seconds
    #df["time-nsec"]  =[datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ' ).timestamp()*1e9 for x in df.time]
    #df["time-msec"]  =[datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ' ).timestamp()*1e3 for x in df.time]
    #df["New_Time_Sec"]  =[datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ' ).timestamp() for x in df.time]
    #df["diff"]= df["New_Time_Sec"]-df["timestamp"]
    #df["IMSI"]=str(df["IMSI"])

    #df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')


    df_Dict_IMEI = df.groupby(['IMEI'])['ProcedureID'].count()

    Dict_IMEI = [x for x in df_Dict_IMEI.index]
    print("len",len(Dict_IMEI))

    Dict=[]
    u = 0
    w=0
    alert_miss=[]
    alert_time=[]
    alert=[]
    count = 0
    df["Alert"]=0.0
    for k in range(0,len(df)):
        #count=0

        i = k
        x = df.index[i]
        #x = pd.to_datetime(x)

        #t=datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S.%f')
        t = x
        K = df.IMEI[i]
        V =str(df.IMSI[i]) 
        T = (V,K)
        
        if T[0]=="0.0":
            
            Dic_Mis={"time" :t, "IMEI" : T[1], "IMSI" : T[0]}
            alert_miss.append(Dic_Mis)
            
                        
            
                        
            
                


               # w=w+1

        if T not in Dict:
            

            w=0
            for items in Dict:
                
                        
                
                if T[0]!="0.0" and T[0] in items:
                    
                    Output = [item[1] for item in Dict if item[0] == T[0] ] 
                    w=w+1
                    print("w",w)
                        #PIMSI = Output[0]
                    PIMEI = Output
                    df.iat[i,df.columns.get_loc("Alert")]= 1.0
                     
                    S_Dic = {"Use Case":"UC2A" ,"time" :str(t) , "IMEI" :str(K) , "IMSI" :str(V) , "Previous IMEI" : PIMEI }
                    alert.append(S_Dic)
                    u=u+1
                    print("u",u)
                    with open(r"share/alerts/{}-UC2A.json".format(datetime.datetime.now().strftime("%Y-%m-%d.%H.%M.%S.%f")),"w") as outfile:

                        
                        json.dump(S_Dic, outfile)
                        outfile.write('\n')
                    
                    
                    
                        







        if T not in Dict:
                #print("No")
                Dict.append(T)

                #print("No, Dict =",Dict)
                #print("u",u)
                #u = u+1
















    #display(print("alert_mis",alert_miss))


    DF_Anomoly_1 =pd.DataFrame(alert)
    DF_Anomoly_1 = pd.DataFrame(alert, columns =['detect-time','IMEI', 'IMSI','Previous IMSI']) 
    #DF_Anomoly_1 = pd.DataFrame.from_records(alert, columns =['Team', 'Age']) 
    DF_Anomoly_1 




    import json

    #data_mis = alert_miss
    #jstr_mis = json.dumps(data_mis, indent=4)
    #display(print(jstr_mis))
    #with open("1st-jupyter-missing-value-Alert-USE-CASE-2B-abc.json", "w") as outfile:
      #for product in data_mis:
        #json.dump(product, outfile)
        #outfile.write('\n')
    


    #Mdf_1.index = pd.to_datetime(Mdf_1.index, format='%Y-%b-%d %H:%M:%S')
    import json

    data = alert
    #print("alert,",alert)
    jstr = json.dumps(data, indent=4)
    print(jstr)

    #with open('Alert-1.csv', 'w') as f:
    #print("Dict",Dict)



    
        
    end = time.time()
    
    print("exec time", end-start)
    if alert:
        
        return outfile

output()