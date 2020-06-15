# -*- coding: utf-8 -*-
"""
Created on Wed May  6 19:07:04 2020

@author: WUIAGA14
"""
import pandas as pd 
from datetime import datetime
import Accuracy_calculate as accuracy_calc
from dateutil.relativedelta import relativedelta
import query_traffic_elastic as elastic_trafic
import model_cuantiles as mod_cuant
import matplotlib.pyplot as plt
from datetime import date

def pipe_model_forecast(df_train,bw,key):
        #Obtener el sesgo de los datos de entrenamiento 
        sesgo=mod_cuant.quartileSkewness (df_train,key)
        sesgo_median=sesgo.dropna().median()
        umbral_list = [sesgo_median for i in range(sesgo.index.shape[0])]
        plt.figure(figsize=(15, 5))
        plt.plot( sesgo.index, sesgo.values, markerfacecolor='blue', markersize=12, color='skyblue', linewidth=2,label='sesgo')
        plt.plot( sesgo.index, umbral_list, markerfacecolor='blue', markersize=12, color='red', linewidth=2,label='median_sesgo')
        
        #Call to the model to return the predictiond and the timestamp asociate to model overcomes banwith 
        fechaSupera, predicciones = mod_cuant.regresionesPercetiles(df_train,[0.95,0.9,0.75,0.5,0.25],2,14,.8)
        #Interpolate quantil 75
        predicciones["interpolate_75"] = predicciones[['0.5','0.25']].apply(lambda row: (2*row['0.5']-(row['0.25']*(sesgo_median+1)))/(1-sesgo_median), axis=1)
        
        predicciones["iqr"]=predicciones[['interpolate_75','0.25']].apply(lambda row: (row['interpolate_75']-(row['0.25'])), axis=1)
        #predicciones["uper_bound"]=predicciones[['0.75','iqr']].loc[fechas.tail(1).index[0]:].apply(lambda row: (row['0.75']+(1.5*row['iqr'])), axis=1)
        predicciones["inter_uper_bound"]=predicciones[['interpolate_75','iqr']].apply(lambda row: (row['interpolate_75']+(1.5*row['iqr'])), axis=1)
        predicciones["inter_lower_bound"]=predicciones[['0.25','iqr']].apply(lambda row: (row['0.25']-(1.5*row['iqr'])), axis=1)
        predicciones["dif_btw_bw"]=predicciones[["inter_uper_bound"]].apply(lambda row: (int(bw)-(row['inter_uper_bound'])), axis=1)
        plt.figure(figsize=(15, 5))
        plt.plot( df_train.index, df_train.values, 'o', alpha=.1, zorder=0,label='Data')
        plt.plot( predicciones.index[0:-1], predicciones["0.75"][0:-1].values, markerfacecolor='blue', markersize=12, color='gold', linewidth=2,label='percetin 0.75')
        plt.plot( predicciones.index[0:-1], predicciones["interpolate_75"][0:-1].values, markerfacecolor='blue', markersize=12, color='red', linewidth=2,label='interpolate_75')
        plt.plot( predicciones.index[0:-1], predicciones["inter_uper_bound"][0:-1].values, markerfacecolor='blue', markersize=12, color='green', linewidth=2,label='inter_upper_bound')
        plt.plot( predicciones.index[0:-1], predicciones["inter_lower_bound"][0:-1].values, markerfacecolor='blue', markersize=12, color='green', linewidth=2,label='inter_lower_bound')
        plt.plot( predicciones.index[0:-1], predicciones["0.95"][0:-1].values, markerfacecolor='blue', markersize=12, color='red', linewidth=2,label='0.95')
        plt.plot( predicciones.index[0:-1], predicciones["0.5"][0:-1].values, markerfacecolor='blue', markersize=12, color='brown', linewidth=2,label='median')
        plt.plot( predicciones.index[0:-1], predicciones["0.25"][0:-1].values, markerfacecolor='blue', markersize=12, color='brown', linewidth=2,label='0.25')
        plt.plot( predicciones.index[0:-1], [int(bw) for i in range(predicciones.index[0:-1].shape[0])], markerfacecolor='blue', markersize=12, color='purple', linewidth=2,label='BW')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        
def pytime_elasticTime(time):
    now = str(time).replace(" ","T")+"Z"
    return now
#A partir de los meses introducidos te devuelve la fecha de hoy y la fecha de los meses trascurrido
def timestamp_elasticsearch_now_n_now(meses):
    now=datetime.now()
    befor_now=now-relativedelta(months=meses)
    return pytime_elasticTime(now),pytime_elasticTime(befor_now)

def timestamp_elasticsearch_timestap_n_month(timestamp,meses):
    now=date.fromisoformat(timestamp)
    befor_now=now-relativedelta(months=meses)
    return pytime_elasticTime(now),pytime_elasticTime(befor_now)

#calculamos la fecha de hoy y la fecha para los meses anteriores añadida now=now,befor_now=now-meses
#elk_timestamp=timestamp_elasticsearch_now_n_now(2)
elk_timestamp=timestamp_elasticsearch_timestap_n_month("2020-04-05",1)
#Query to elasticsearch to from the dataset
#request trafic for sabm
#reque_trafic=elastic_trafic.request_trafic_data(elk_timestamp[1],elk_timestamp[0],"sabm-col-baq-mw-m-386781_GigabitEthernet0/0.1100")
#reque_trafic=elastic_trafic.request_trafic_data(elk_timestamp[1].replace("Z", ""),elk_timestamp[0].replace("Z", ""),"cmex-mex-occrtriw01_GigabitEthernet0/0/0")
#reque_trafic=elastic_trafic.request_trafic_data(elk_timestamp[1].replace("Z", ""),elk_timestamp[0].replace("Z", ""),"bbva-ven-ccs-mp-m-035864_GigabitEthernet0/1")
#reque_trafic=elastic_trafic.request_trafic_data(elk_timestamp[1].replace("Z", ""),elk_timestamp[0].replace("Z", ""),"bbva-col-bog-mw-2-1004210_TenGigabitEthernet0/1/0.112")
reque_trafic=elastic_trafic.request_trafic_data(elk_timestamp[1].replace("Z", ""),elk_timestamp[0].replace("Z", ""),'bbva-col-bog-mw-2-1004210_TenGigabitEthernet0/1/0.101')
	

#dataset trafic 
df_traffic=elastic_trafic.trafic_request_to_df(reque_trafic)
#Clean dataset to 
df_traffic.set_index(pd.DatetimeIndex(df_traffic['data_time']),inplace=True)
df_train=df_traffic[["Inbps","Outbps"]].astype("int64")

#Splite the dataset in train and test 
'''
train=df_train.iloc[:][:-int(df_train.shape[0]/3)]
test=df_train.iloc[:][-int(df_train.shape[0]/3):]

#Dias de entrenamiento restamos el primer y ultimo dia del dataset
days_trainin=(df_train.tail(1).index[0].to_pydatetime()-df_train.head(1).index[0].to_pydatetime()).days
#Seleccionamos la fecha del indice por la que vamos a trocer los datos para entrenamiento y testeo
train_test_date_time=df_train.head(1).index[0].to_pydatetime() + relativedelta(days=(days_trainin-days_trainin/3))
#Seleccionamos los datos de entrenamiento y testeo
train=df_train.loc[:str(train_test_date_time)[:-3]]
test=df_train.loc[str(train_test_date_time)[:-3]:]
#Drop first row of test becaus its repeated
test.drop(test.head(1).index,inplace=True)
'''
pipe_model_forecast(df_train["Inbps"],df_traffic["InBandwidth"][0],"Inbps")
pipe_model_forecast(df_train["Outbps"],df_traffic["OutBandwidth"][0],"Outbps")
'''
percentil=0.95
test=test[["Inbps"]].resample('1d').apply(lambda x: x.quantile(percentil))
predicciones=predicciones.resample('1d').apply(lambda x: x.quantile(percentil))
merge=pd.merge(test,predicciones, how='inner', left_index=True, right_index=True)
merge=merge.dropna()
merge["0.95"]=merge["0.95"].abs()
#Evaluate the model 

accuracy_calc.ploty_real_forecast(merge['Inbps'], merge['0.95'],merge.index)
variance_score,mx_err,mae,mad,mape,rmase,r2,medae,msle=accuracy_calc.accuracy_forecast(merge['Inbps'], merge['0.95'])
'''