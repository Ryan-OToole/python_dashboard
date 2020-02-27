# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 23:47:36 2020

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

        #T-mob-USE-CASE-1A -Procedure ID =6
    # Tahereh MAzaheri

    #import libraries
    import warnings
    import itertools
    import numpy as np
    import matplotlib.pyplot as plt
    warnings.filterwarnings("ignore")
    plt.style.use('fivethirtyeight')
    import pandas as pd
    import statsmodels.api as sm
    import matplotlib
    matplotlib.rcParams['axes.labelsize'] = 14
    matplotlib.rcParams['xtick.labelsize'] = 12
    matplotlib.rcParams['ytick.labelsize'] = 12
    matplotlib.rcParams['text.color'] = 'k'

    #Read data

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import influxdb
  
    #import seaborn as sns
    #df = pd.read_csv(r"C:\Users\tmazaherikou\Documents\abc.csv")

    df.head()
    df

    import time
    from datetime import datetime, tzinfo, timedelta
    import arrow
    import datetime

  
    
    
    df["time_3"] = [datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ') for x in df.time]
    df["time"] = df["time_3"]
    df.set_index('time', inplace=True, drop=True)

    df = df.rename(columns = {"Procedure ID":"ProcedureID" ,"Current Cell ID":"Current_Cell_ID" }) 

    #df.drop("Unnamed: 0", axis =1 , inplace = True)

    df_Dict_IMEI = df.groupby(['IMEI'])['ProcedureID'].count()



    type(df.index[0])
    df_Dict_IMEI.max()
    df_Dict_IMEI.mode()

    Dict_IMEI = [x for x in df_Dict_IMEI.index]
    Dict_IMEI

    df.head()

    df.isna().sum()

    df = df.fillna(0)
    df.isna().sum()





    df1= df[df["ProcedureID"]==6]
    df1.head()




    df1t=df1.ProcedureID.resample("5T").count()
    Mdf1 = df1t.to_frame().reset_index()
    Mdf1

    plt.plot(df1t)

    Mdf1 = df1t.to_frame().reset_index()
    df_M = Mdf1





    # index data with "time" (Since the data is time serries, the order of data should not be changed by doing this step)
    df_M= df_M.set_index('time')
    df_M.index
    df_M.head()
    #y = df['Count'].resample('MS').mean()
    #y.plot(figsize=(15, 6))
    #plt.show()


    #from pylab import rcParams
    #rcParams['figure.figsize'] = 18, 8
    #decomposition = sm.tsa.seasonal_decompose(df, model='additive')
    #fig = decomposition.plot()
    #plt.show()


    df_M.plot(figsize=(15, 6))
    plt.show()

    p = d = q = range(0, 2)
    pdq = list(itertools.product(p, d, q))
    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]
    print('Examples of parameter combinations for Seasonal ARIMA...')
    print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[1]))
    print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[2]))
    print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[3]))
    print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[4]))

    # Find the best parameter for the model, (Choose the parameters with the lowest AIC)
    #df_param = pd.DataFrame( columns=['param','param_seasonal', 'AIC'])



    #%%time
    import warnings
    warnings.filterwarnings('ignore')

    # Find the best parameter for the model, (Choose the parameters with the lowest AIC)
    df_param = pd.DataFrame( columns=['param','param_seasonal', 'AIC'])

    par_lis=[]
    par_s_lis=[]
    AIC_lis=[]
    for param in pdq:
        for param_seasonal in seasonal_pdq:
            try:
                mod = sm.tsa.statespace.SARIMAX(df_M,order=param,seasonal_order=param_seasonal,enforce_stationarity=False, enforce_invertibility=False)
                results = mod.fit()
                #print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
                par_lis.append(param)
                par_s_lis.append(param_seasonal)
                AIC_lis.append(results.aic)
            except:
                continue


    list_of_tuples=list(zip(par_lis, par_s_lis, AIC_lis))
    # Converting lists of tuples into 
    # pandas Dataframe. 

    df_param =pd.DataFrame(list_of_tuples, columns=['param','param_seasonal','AIC'])

    df_param.head()

    df_param[df_param["AIC"]==df_param.AIC.min()]

    #fit the model
    order =df_param.iloc[df_param["AIC"].idxmin()]["param"]
    print(order)

    param_seasonal =(0,0,0,0)
    #df_param.iloc[df_param["AIC"].idxmin()]["param_seasonal"]
    print(param_seasonal)
    mod = sm.tsa.statespace.SARIMAX(df_M,
                                    order=order,
                                    seasonal_order=param_seasonal,
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)
    results = mod.fit()
    print(results.summary().tables[1])

    results.plot_diagnostics(figsize=(16, 8))
    plt.show()

    #Predict
    pred = results.get_prediction(start=5, dynamic=False)
    pred_ci = pred.conf_int()
    ax = df_M.plot(label='observed')
    pred.predicted_mean.plot(ax=ax, label='One-step ahead Forecast', alpha=.7, figsize=(14, 7))
    ax.fill_between(pred_ci.index,
                    pred_ci.iloc[:, 0],
                    pred_ci.iloc[:, 1], color='g', alpha=.2)
    ax.set_xlabel('Time')
    ax.set_ylabel('Count')
    plt.legend()
    plt.show()

    pred_d = results.get_prediction(start=5, dynamic=True)
    pred_ci = pred_d.conf_int()
    ax = df_M.plot(label='observed')
    pred_d.predicted_mean.plot(ax=ax, label='One-step ahead Forecast', alpha=.7, figsize=(14, 7))
    ax.fill_between(pred_ci.index,
                    pred_ci.iloc[:, 0],
                    pred_ci.iloc[:, 1], color='g', alpha=.2)
    ax.set_xlabel('Time')
    ax.set_ylabel('Count')
    plt.legend()
    plt.show()

    pred_ci

    df_M

    pred.predicted_mean.plot()


    pred_d.predicted_mean.plot()

    print(pred.predicted_mean)
    #pred.pedicted_mean.max(),
    pred_ci.max()

    Upperband = pred_ci["upper ProcedureID"].mean()
    Upperband = pred_ci["upper ProcedureID"].min()
    #######################################################################################################
    df_lis=[]
    df_1 = []
    df_6 =[]
    df_count1 =[]
    df_count6 =[]

    for i in range(0,len(Dict_IMEI)):

        IME = Dict_IMEI[i]
        #print("IME",IME)
        delta_f = df[df["IMEI"] == IME]
        Mdf1 = delta_f[delta_f["ProcedureID"]==6]
        
        df_lis.append(delta_f)
        df_1.append(Mdf1)
        
        Mdf_1=Mdf1.ProcedureID.resample("5T").count()
        
        df_count1.append(Mdf_1)
        
    #print(IME)##########################################################################################
    Df_Count1=[]
    
    for i in range(0,len(Dict_IMEI)):
        Df_Count1.append(df_count1[i].to_frame().reset_index())

        
    ##########################################################################################################
    Outlier_1=[]
    Dic= {}
    import datetime
    import json
    for i in range(0,len(Dict_IMEI)):


        for j in range(0,len(Df_Count1[i])):


            x = Df_Count1[i].ProcedureID[j]
            t = Df_Count1[i].time[j]
            t2 = datetime.datetime.strftime(t, '%Y-%m-%d %H:%M:%S.%f')
            #print(x)
            if x> Upperband:
                print(x,t2, "i = ", i, "IMEI =",Dict_IMEI[i])
                Dic = { "Use Case" : "UC1B-Attach","time" : str(t2), "IMEI":str(Dict_IMEI[i]), "UE Count": str(x) }
                Outlier_1.append(Dic)
                #print("dic", Dic)
                with open(r"share/alerts/{}-UC1B-Detach.json".format(datetime.datetime.now().strftime("%Y-%m-%d.%H.%M.%S.%f")),"w") as outfile:
                #with open(r"share/alerts/{}-UC1A-Attach.json".format(datetime.datetime.now()),"w") as outfile:

                    json.dump(Dic, outfile)
                    outfile.write('\n')
                    
    DF_Anomoly_1 =pd.DataFrame(Outlier_1)
    DF_Anomoly_1 
    ########################################################################################################
    
    
    ########################################################################################################
 
      



    #####################################################################################################################
    import json

    data = Outlier_1
    jstr = json.dumps(data, indent=4)
    print(jstr)


    


        
    end = time.time()
    
    print("exec time", end-start)
    if Outlier_1:
        
        return outfile

output()