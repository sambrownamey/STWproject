# -*- coding: utf-8 -*-
"""
Created on Wed Dec 14 13:25:24 2016

This script reads in the GISST data on valve ops and burst mains.

@author: 142796
"""

from stwpy.dmautils import fix_dma
import pandas as pd
import os

# Currently, below folder needs to contain files called 'ValveOperations.csv', 
# 'ArchivedValveOps.csv', 'ArchivedBurstMains.csv', 'BurstMains.csv', 
# 'SAP_Mains_Repairs.csv'

#GISST_SAP_Op_path = ('..\\..\\04_Background_Documents_Data\\00. MI Analysis\\'
#                     'Data\\GISST Data\\GISST\\SAP Operational Data\\')

GISST_SAP_Op_path = ('S:/00_Projects/CO00690090_STW WQ Analytics/'
                     '04_Background_Documents_Data/00. MI Analysis/'
                     'Data/GISST Data/GISST/SAP Operational Data/')

GVOPicklePath = 'PickledData/GisstValveOps.pkl'
GBMPicklePath = 'PickledData/GisstBurstMains.pkl'



def read_gisst_csv(filepath, **kwargs):
        DF = pd.read_csv(filepath, **kwargs)
        DF['SourceFile'] = filepath.split(os.sep)[-1]
        if "WORK_ORDER" in DF.columns:
            if "OPERATION_NUMBER" not in DF.columns:
                DF['OPERATION_NUMBER'] = '00'
            DF["OrderAndOp"] = DF[['WORK_ORDER', 'OPERATION_NUMBER']].apply(
               order_with_opn, 
               axis = 1)
        return DF

def gisst_date_parser(s):
    if s == '':
        return pd.NaT
    else:
        return pd.to_datetime(s, 
                              format = "%d/%m/%Y %H:%M:%S",
                              errors = 'raise')
        
burst_main_datecols = [
    'JOB_STATUS_DATE',
    'EARLIEST_START_DATE',
    'LATEST_FINISH_DATE', 
    'OP_ACTUAL_FINISH_DT',
    'DATE_CREATED', 
    'DATE_RELEASED_SCHED', 
    'DATE_DISPATCHED', 
    'DATE_ACCEPTED',
    'DATE_ARRIVED',
    'DATE_PARKED',
    'DATE_REJECTED',
    'DATE_SUSPEND',
    'DATE_COMPLETED',
    'DATE_CANCELLED' 
    ]
    
valve_op_datecols = [
    'VALVE_STATUS_DATE',
    'DATE_POTENTIAL',
    'DATE_CREATED', 
    'DATE_RELEASED_SCHED', 
    'DATE_DISPATCHED', 
    'DATE_ACCEPTED',
    'DATE_ARRIVED',
    'DATE_PARKED',
    'DATE_REJECTED',
    'DATE_SUSPEND',
    'DATE_COMPLETED',
    'DATE_CANCELLED' 
    ]
    
mr_datecols = [
    'FWR_CREATE_DT',
    'OP_ACTUAL_FINISH_DT'
    ]
    
big_datecols = [
    'EARLY_START_DATE',
    'LATEST_FINISH_DATE',
    'ACTUAL_FINISH_DATE',
    'STATUS_DATE',
    'CREATED_DATE'
    ]

def order_with_opn(row):
    try:
        return "/".join([str(row[0]), str(row[1])])
    except:
        pass    

def read_gisst_valve_ops():
    output = []
    for name in ['ValveOperations.csv', 'ArchivedValveOps.csv']:
        filepath = GISST_SAP_Op_path + name
        output.append(
            read_gisst_csv(filepath, 
                           parse_dates = valve_op_datecols,
                           date_parser = gisst_date_parser,
                           dtype = {'DMA_ID1': str,
                                    'DMA_SAP_CODE1': str,
                                    'DMA_ID2': str,
                                    'DMA_SAP_CODE2':str},
                           thousands = ',')
                     )
    df = output[0].append(output[1])
    return df
    
def read_gisst_burst_mains():
    output = []
    for name in ['ArchivedBurstMains.csv', 'BurstMains.csv']:
        filepath = GISST_SAP_Op_path + name
        output.append(
            read_gisst_csv(filepath, 
                           parse_dates = burst_main_datecols,
                           date_parser = gisst_date_parser,
                           dtype = {'DMA_CODE': str,
                                    'DMA_SAP_CODE': str},
                           thousands = ',')
                     )
    df = output[0].append(output[1])
    return df
    
def read_gisst_sapmr():
    name = "SAP_Mains_Repairs.csv"
    filepath = GISST_SAP_Op_path + name
    output=pd.read_csv(filepath, 
                       parse_dates = mr_datecols,
                       date_parser = gisst_date_parser,
                       #dtype = {'DMA_CODE': str,
                       #         'DMA_SAP_CODE': str},
                       thousands = ',')
    output['SourceFile'] = name
    return output
    
def read_gisst_sap_big():
    name = "GISSTSAPWOO.csv"
    folder = "../../04_Background_Documents_Data/"
    df = pd.read_csv(folder + name, 
                     parse_dates = big_datecols,
                     date_parser = gisst_date_parser,
                     thousands = ',')
    return df

    
if __name__ == "__main__":
    GVODF = read_gisst_valve_ops()
    GVODF = GVODF.drop(['DMA_ID2', 'DMA_NAME2', 'DMA_SAP_CODE2'], 
                       axis = 'columns')
    GVODF['DMA'] = GVODF['DMA_ID1'].apply(fix_dma)
    GVODF.to_pickle(GVOPicklePath)
    
    GBMDF = read_gisst_burst_mains()
    GBMDF['DMA'] = GBMDF['DMA_CODE'].apply(fix_dma)
    GBMDF.to_pickle(GBMPicklePath)
            
else:
    try:
        GVODF = pd.read_pickle(GVOPicklePath)
        GBMDF = pd.read_pickle(GBMPicklePath)
    except FileNotFoundError as e:
        print("Error: pickle file not found. Try running newvopmerger.py.")
        raise e
