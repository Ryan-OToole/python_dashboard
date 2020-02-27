# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 11:48:08 2020

@author: tmazaherikou
"""

#Tahereh Mazaheri @ DTC_NOla

# This script reads location from the geoservice URL: should be run with Geoservice URl
#pip install geopy
import time
start = time.time()

def output3():
    
    from geopy.distance import geodesic
    from time import sleep
    import pandas as pd
    import numpy as np
    import urllib.request
    import datetime
    import influxdb


    
    
    #client = influxdb.InfluxDBClient('localhost', 8086, database = 'dummydb')
    #q = "select * from abc1 limit 100"
    
    #df = pd.DataFrame(client.query(q).get_points())
                     
    df = pd.read_csv(r"C:\Users\tmazaherikou\Documents\abc.csv")                 
    df.head()
    df.rename(columns={'Current Cell ID': 'Current_Cell_ID', 'Procedure ID': 'ProcedureID'}, inplace=True)
    df.head()
  
    display(print(df))
 
    
    
    df["LAT"] = 0.0 
    df["LON"] = 0.0
    #df["LOC"] = 0.0
    df["Dist-km"] = 0.0
    df["Speed-km/h"] = 0.0
    df["delt-s"]=0.0
    df["delt-h"]=0.0
    df["Alert"]=0.0
    df.head()

    df.isna().sum()

    df = df.fillna(0)
    df.isna().sum()

 

    
    
    
    
    #print(type(df.time[0]))

    
  

    df_Dict_IMEI = df.groupby(['IMEI'])['ProcedureID'].count()



    

    Dict_IMEI = [x for x in df_Dict_IMEI.index]
    display(print("Len(Dict_IMEI)", len(Dict_IMEI),"Dict-IMEI",Dict_IMEI))

    
    #type(df.index[1])
    

    #print(User)

    # Group the data with IMEI, since the data is chronogically ordered, gouping the data should give a time serries for each IMEI
    # for each IMEI in dictionary, make a group

    df_lis=[]

    for i in range(0,len(Dict_IMEI)):

        IME = Dict_IMEI[i]
        delta_f = df[df["IMEI"] == IME]
        df_lis.append(delta_f)

        #print(IME)
    #print(len(df_lis))
    #print(len(Dict_IMEI))


    for k in range(0,len(Dict_IMEI)):
        df_lis[k]=df_lis[k].reset_index()
        
            
    display(print(df_lis[k].head()))   
 
    
    
    for k in range(0,len(Dict_IMEI)):
        
        print("k=",k)
        x =0
        for i in range (0,len(df_lis[k]),1):
            #print("i=",i)
            y = df_lis[k]["Current_Cell_ID"][i]
            #display(print("y",y))
            #print("y",y)
            if y!=x: 

                    #cellid="111:222:333:444:555"
                    #c1=int(random.uniform(111,  555))
                    #c2=int(random.uniform(111,  555))
                    #cellid="111:222:333:"+str(c1)+":"+str(c2)
                    cellid= df_lis[k]["Current_Cell_ID"][i]
                    print("celllid","i,",cellid,i)
                    latlon_str = urllib.request.urlopen("http://localhost:19111/{cellid}".format(cellid=cellid)).read().decode()
                    #!!http://localhost:19111/%22111:222:333:444:666%22
                    #latlon_str = urllib.request.urlopen("http://localhost:19111/%22111:222:333:444:666%22".format(cellid=cellid)).read().decode()

                    lat_str,lon_str=latlon_str.split(",")
                    lat,lon=float(lat_str),float(lon_str)
                    print(lat,lon)
                    df_lis[k].iat[i,df_lis[k].columns.get_loc("LAT")]= float(lat )
                    df_lis[k].iat[i,df_lis[k].columns.get_loc("LON")]= float(lon)
                    
            if y==x:

                    df_lis[k].iat[i,df_lis[k].columns.get_loc("LAT")]= float(lat )
                    df_lis[k].iat[i,df_lis[k].columns.get_loc("LON")]= float(lon)
                    print("NewCellid =oldCellid",cellid, lat,lon )
            x=y        
                    

    
    
    lis_k=[]
    Dic={}
    lis_Dic=[]
    from geopy.distance import geodesic
    import datetime
    import json
    for k in range(0,len(Dict_IMEI)):
        for i in range (1,len(df_lis[k]),1):

            j=i-1

            lat =  df_lis[k].iat[j,df_lis[k].columns.get_loc("LAT")]
            lon = df_lis[k].iat[j,df_lis[k].columns.get_loc("LON")] 
            t = df_lis[k].iat[j,df_lis[k].columns.get_loc("timestamp")] 
            #time =datetime.datetime.strftime(df_lis[k].index[j], '%Y-%m-%d %H:%M:%S.%f') 
            time_1 =  df_lis[k].iat[j,df_lis[k].columns.get_loc("time")] 
            Loc =(lat,lon)


            lat1 =  df_lis[k].iat[i,df_lis[k].columns.get_loc("LAT")]
            lon1 = df_lis[k].iat[i,df_lis[k].columns.get_loc("LON")]
            Loc1 =(lat1,lon1)
            t1 = df_lis[k].iat[i,df_lis[k].columns.get_loc("timestamp")] 
            #time_1 = datetime.datetime.strftime(df_lis[k].index[i], '%Y-%m-%d %H:%M:%S.%f')
            time_2 =  df_lis[k].iat[i,df_lis[k].columns.get_loc("time")]
            Dist = geodesic(Loc, Loc1).km

            df_lis[k].iat[i,df_lis[k].columns.get_loc("Dist-km")]= Dist 

            delt = t1-t
            df_lis[k].iat[i,df_lis[k].columns.get_loc("delt-s")]= delt

            delt_h = delt/3600.0

            df_lis[k].iat[i,df_lis[k].columns.get_loc("delt-h")]= delt_h


            Speed = float(Dist/delt_h)
            df_lis[k].iat[i,df_lis[k].columns.get_loc("Speed-km/h")]= Speed


            if Speed > 500:
                Alert = 1.0
                print("IMEI=",Dict_IMEI[k],"speed=", Speed, "Previous Location ", Loc, "Previous time ",time_1, "Present Location ", Loc1,"Present time" , time_2)
                Dic ={"Use Case":"UC3", "IMEI=":Dict_IMEI[k],"speed=": Speed, "Previous Location ": Loc, "Previous time ":time_2, "Present Location ": Loc1,"Present time": time_1}
                lis_Dic.append(Dic)
                df_lis[k].iat[i,df_lis[k].columns.get_loc("Alert")]= 1.0
                lis_k.append(k)
                print("k",k)
                with open(r"share/alerts/{}-UC3.json".format(datetime.datetime.now().strftime("%Y-%m-%d.%H.%M.%S.%f")),"w") as outfile:
   
                    json.dump(Dic, outfile)
                    outfile.write('\n')
#Mdf_1.time = pd.Timestamp(Mdf_1.time)
    display(print(df_lis[1]))    
    #lis_Dic

#Mdf_1['time'] = pd.to_datetime(Mdf_1['time'], format='%Y-%m-%dT%H:%M:%SZ')
    
    
    
    
    
    import json

    data = lis_Dic
    jstr = json.dumps(data, indent=4)
    print(jstr)

    
################################################################################################################################

    #Mdf_lis=[] 
    
#/share/alerts/2020-02-12-063012-UC2a-XXX.json
#/share/alerts/2020-02-12-071117-UC1b-XXX.json
#/share/alerts/2020-02-12-072352-UC1a-XXX.json
#/share/alerts/2020-02-12-084016-UC1b-XXX.json
#/share/alerts/2020-02-12-091112-UC2a-XXX.json
#/share/alerts/2020-02-12-101731-UC1a-XXX.json

 
    
    #df_lis[1].set_index('time', inplace=True, drop=True)
    #display(print("df_lis[1] after time index"),df_lis[1])
    #import time
    #from datetime import datetime, tzinfo, timedelta
    #import arrow
    #import datetime

    
    #for k in range(0,len(Dict_IMEI)):
        #df_lis[k].set_index('time', inplace=True, drop=True)
    #for k in range(0,len(Dict_IMEI)):   
       # df_lis[k]["dt"]=[arrow.get(x) for x in df_lis[k].index]
        #display(print(df_lis[k].index[0]))
    #for k in range(0,len(Dict_IMEI)):  
       # df_lis[k]["dt2"]=[str(x.date())+str(x.time()) for x in df_lis[k].dt]
    #for k in range(0,len(Dict_IMEI)): 
       
        #df_lis[k]["dt3"]=[datetime.datetime.strptime(x, '%Y-%m-%d%H:%M:%S.%f') for x in df_lis[k].dt2]
    #for k in range(0,len(Dict_IMEI)):  
        #df_lis[k].set_index('dt3', inplace=True, drop=True)
        
  
    
    #for k in range(0,len(Dict_IMEI)):
        #df_lis[k].set_index('time', inplace=True, drop=True)
        
        #print("dffff",df_lis[k])
        #Mdf_lis.append(df_lis[k].Alert.resample("5T").sum())

#AMdf_1 = AMdf_1.to_frame().reset_index()
#
   # dMdf = []
   # for k in range(0,len(Dict_IMEI)):
    
       # dMdf.append(Mdf_lis[k].to_frame().reset_index())




    #display(print("dMdf",dMdf[3]))




#make a csv and json file to record alerts 
   # import datetime

   # Alert_list=[]
   # Alert_list_2=[]
   # Dic={}

  #  for k in range(0,len(Dict_IMEI)):
       # Dic={}
    #Alert_list=[]
    #T =( "IMEI-No. =", k, "IMEI=",Dict_IMEI[k])
    #Alert_list.append(T)
        #for i in range (0,len(dMdf[k])):
        
            #x=dMdf[k]["Alert"][i]
           # t= dMdf[k]["dt3"][i]
           # t2= datetime.datetime.strftime(t, '%Y-%m-%d %H:%M:%S.%f')
           # if x> 0:
                #t= pd.to_datetime(t)
                #t2= datetime.datetime.strftime(t, '%Y-%m-%d %H:%M:%S.%f')
                #Dic = {"ALEEEERRRTTTTT": "Please see the time bin and IMEI for this Alert", "time" : t2 , "Alert": x, "IMEI" : Dict_IMEI[k], "IMEI-INDEX" : k}
                #Alert_list.append(Dic)
            
    #Alert_list_2.append(Alert_list)
        
    
        
    

#Mdf_1.index = pd.to_datetime(Mdf_1.index, format='%Y-%b-%d %H:%M:%S')
    #import json

    #data = Alert_list_2
    #jstr = json.dumps(data, indent=4)
    #print(jstr)

#with open('Alert-1.csv', 'w') as f:
    



    #with open("BBBin-Alert-USE-CASE-3-abc.json", "w") as outfile:
      #for product in Alert_list_2:
        #json.dump(product, outfile)
        #outfile.write('\n')






    
    

        
        
        


                    
                    




    
        
    end = time.time()
    
    print("exec time", end-start)
    
    if lis_Dic:
        return outfile

output3()