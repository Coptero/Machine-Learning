# -*- coding: utf-8 -*-
"""
Created on Wed May  6 17:13:20 2020

@author: WUIAGA14
"""

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers 
import pandas as pd 
from datetime import datetime
try:
    es = Elasticsearch(['https://copt-ml-read:qwerty123456@traful-apielastic.wh.telefonica'],
     use_ssl=True,
    # make sure we verify SSL certificates
    verify_certs=False)
    print ("Connected", es.info())
except Exception as ex:
 print ("Error:", ex)
 
def request_trafic_data(start,end,host_intf):
    q={
      "query": {
            "bool": {
              "must": [
                {
                  "range": {
                    "LastSync": {
                      "format": "strict_date_optional_time",
                      "gte": start,
                      "lte": end
                    }
                  }
                }
              ],
              "filter": [
                {
                  "bool": {
                    "should": [
                      {
                        "match_phrase": {
                          "hostname_interface": host_intf
                        }
                      }
                    ],
                    "minimum_should_match": 1
                  }
                }
              ],
              "should": [],
              "must_not": []
            }    
            },
   "sort" : [
      {"LastSync" : {"order" : "asc"}}
   ]
    };
    
    request=helpers.scan(es, query=q, scroll='1h', raise_on_error=True, preserve_order=True, index = "copt-sw-traffic*")
    return request
def trafic_request_to_df(trafic_records):
    df_traffic = pd.DataFrame() 
    
    dic={'timestamp':[],'Outbps':[],'Inbps':[],'InBandwidth':[],'data_time':[],'OutBandwidth':[]}
    for i in trafic_records:
        dic['data_time'].append(datetime.strptime(i['_source']['LastSync'], '%Y%m%d%H%M%S'))
        dic['timestamp'].append(i['_source']['LastSync'])
        dic['Inbps'].append(i['_source']['Inbps'])
        dic['Outbps'].append(i['_source']['Outbps'])
        dic['InBandwidth'].append(i['_source']['InBandwidth'])
        dic['OutBandwidth'].append(i['_source']['OutBandwidth'])
        #forecat_ETL_record.append(dic)
    for k,v in dic.items():
        df_traffic[k]=v 
    
    return df_traffic
