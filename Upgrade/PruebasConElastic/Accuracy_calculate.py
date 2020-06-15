# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 11:24:16 2020

@author: WUIAGA14
"""

import sklearn.metrics as sk_metrics
import numpy as np
import matplotlib.pyplot as plt

def ploty_real_forecast(y_true, y_pred,time):
    plt.figure(figsize=(15, 5))
    plt.plot( time, y_true, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=2,label='Real_data')
    plt.plot( time, y_pred, marker='*', markerfacecolor='purple',markersize=12, color='plum', linewidth=2,label='Forecast_data')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

def mean_absolute_percentage_error(y_true, y_pred): 
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    aux=np.abs((y_true - y_pred) / y_true)
    aux = aux[~np.isnan(aux)]
    aux =aux[np.isfinite(aux)]
    mape= np.mean(np.abs(aux)) * 100
    return mape
def mean_absolute_deviation(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    aux=np.abs((y_true - y_pred) / y_true)
    aux = aux[~np.isnan(aux)]
    aux =aux[np.isfinite(aux)]
    median=np.median(aux)
    mad=np.abs(aux - median)
    return np.median(mad)
def accuracy_forecast(real_data,forecast_data):
    #Referencias -> LONG-RANGE_FORECASTING_FROM_CRISTAL_BALL_TO_COMPUTER_Optimised_edited.pdf -> page 346
    #coeficiente de variacion, informa acerca de la dispersión relativa de un conjunto de datos, expresa la desviación estándar como porcentaje de la media aritmética
    variance_score=sk_metrics.explained_variance_score(real_data,forecast_data)
    #max_error 
    mx_err=sk_metrics.max_error(real_data,forecast_data)
    #mae refleja el error tipico/medio absoluto evitando que los errores negativos y positivos influyan en el error total
    mae=sk_metrics.mean_absolute_error(real_data,forecast_data)
    #Mean absolute deviation -> refleja el error tipico sin distincion entre varianza y bias -> se utiliza cuando el valor del error es lineal
    mad=mean_absolute_deviation(real_data,forecast_data)
    #Median absolute error 
    medae=sk_metrics.median_absolute_error(real_data,forecast_data)
    #mean squared log error 
    msle=sk_metrics.mean_squared_log_error(real_data,forecast_data)
    #Utilizar cuando la funcion de perdida sea cuadratica, el coste asociado al error aumenta de forma cuadratica, proboca grades erorres de medicion
    rmase=sk_metrics.mean_squared_error(real_data,forecast_data)
    #Sencillo y util para comparar forecasting con diferentes unidades metricas, very convenient when you want to explain the quality of the model to your management
    mape=mean_absolute_percentage_error(real_data,forecast_data)
    #coefficient of determination -> 
    '''
    Un R 2 de 0 significa que la variable dependiente no se puede predecir a partir de la variable independiente.
    Un R 2 de 1 significa la variable dependiente se puede predecir sin error de la variable independiente.
    Un R 2 entre 0 y 1 indica el grado en que la variable dependiente es predecible. Un R 2 de 0,10 medios que 10 por ciento de la variación en Y es predecible a partir de X ; un R 2 de 0,20 significa que el 20 por ciento es predecible; y así.
    '''
    r2=sk_metrics.r2_score(real_data,forecast_data)
    return variance_score,mx_err,mae,mad,mape,rmase,r2,medae,msle
