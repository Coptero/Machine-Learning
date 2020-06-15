# -*- coding: utf-8 -*-
"""
Created on Wed May  6 19:14:46 2020

"""
import pandas as pd 
from datetime import timedelta
import numpy as np
import statsmodels.formula.api as smf
from dateutil.relativedelta import relativedelta

#Resample and select the quantle
def quantile_df(df,quantile):
    return df.resample('1h').apply(lambda x: x.quantile(quantile))
#Sesgo de los cuantiles 
def quartileSkewness (serie,UB=0.75,M=0.5,LB=0.25):
    ub=quantile_df(serie,UB).dropna()
    m=quantile_df(serie,M).dropna()
    lb=quantile_df(serie,LB).dropna()
    C = (ub+lb-2*m)/(ub-lb)
    #C = (lb-(ub-lb))/(14*0.8)
    #C = np.median(C)
    return C.dropna()

#Input
#Serie -- tiene formato index=fecha, valor=trafico
#Cuantiles --  array con los cuantiles a representar
#meses -- Meses a calcular
#caudalM -- valor del caular maximo 
#porcentaje -- porcentaje sobre el caudalMaximo que se considera congestion si se supera
#TipoFun -- tipo de funcion que se aproxima, por defecto es lineal, log para log(x)

#Outputs
#fechaSupera -- Dataframe con los cantiles y las fechas que  las prediciones de tendencia superan caudalM*porcentaje
#predicciones -- valores de la prediccion de tendencia

def regresionesPercetiles (serie,cuantiles,meses,caudalM,porcentaje,tipoFun='lineal'): 
    #Indices y datos a usar en el modelo, numero de registros 
    a= serie.shape[0]
    #
    index = np.arange(1,a+1)
   
    #Eje x no puede ser timestamp, por lo que se genera uno auxiliar
    x = np.reshape(index, (a,1))
    
    #El eje y son los valores de la serie con los datos del tráfico
    y = serie.iloc[:,[0]].values[:,0]
    
    
    #Guardamos los nuevos datos para el modelo 
    data = {'x': x , 'y': y}    
    #Extraemos la ultima fecha que tengamos registros
   
    lst_date_info=serie.tail(1).index[0].to_pydatetime()
    #A partir de la fecha del ultimo registro obtemos la ultima fecha a predecir -> lst_date_info+meses
    pred_date_time=lst_date_info + relativedelta(months=meses)
    #Calculamos la diferencia de dias entre las fechas
    dys_last_info_and_pred_date=(pred_date_time-lst_date_info).days
    a=a+288*dys_last_info_and_pred_date
    
    #Multiplicamos el numero de meses por los puntos en un dia y los dias en un mes
    #a=a+meses*288*30
    
    #Valores para la predicción
    z=np.arange(1,a)
    #Creamos una lista de fechas desde la primera de la serie (entendiendo que es la posición 0 del index) hasta la ultima
    #formada por el numero de valores de la serie mas los puntos a predecir 
    date_list = [serie.index[0] + timedelta(minutes=5*x) for x in range(0, a)]
    fechas=pd.to_datetime(date_list)
    
    #Definicion del modelo
    if tipoFun == 'log':
        mod = smf.quantreg('y ~ I(np.log(x))', data)
    else: 
        mod = smf.quantreg('y ~ x', data)
    #Entrenamos el modelo con cada uno de los cuantiles
    res_all = [mod.fit(q=q) for q in cuantiles]
    #Dataframe con los datos a devolver
    fechaSupera=pd.DataFrame({'cuantiles': [] , 'fechas': []})
    valoresq=[]
    valoresf=[]
    #DataFrame con las fechas cuantil y valores
    predicciones= pd.DataFrame({'fechas': fechas[:-1]})
    for qm, res in zip(cuantiles, res_all): 
        #Transformo qm a string para que sea el valor de la columna
        predicciones[str(qm)]=res.predict({'x': z})
        # cogemos las predicciones por cada modelo y predecimos
         
        #Si los valores de prediccion superan el valor caudalMaximo * porcentaje se guardan
        for index,value in enumerate(res.predict({'x': z})):
            if value >= caudalM*porcentaje:
                valoresq.append(qm)
                valoresf.append(fechas[index].date())
                break
    fechaSupera['cuantiles']=valoresq
    fechaSupera['fechas']=valoresf
    predicciones.set_index('fechas', inplace = True) #Lo seteo como indice 
    
    #Esta funcion en la version final no estara 
    #representacionGrafica(serie,predicciones,caudalM,porcentaje)
    
    return fechaSupera, predicciones