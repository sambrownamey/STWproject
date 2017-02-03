# -*- coding: utf-8 -*-
"""
This script reads in the data from Jon Hagan's infra model. 

@author: 142796
"""

import pandas as pd
from stwpy.dmautils import fix_floc_dma_id

# Defining the paths and files containing the data we need

path = 'S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/00. MI Analysis/Data/Infra Maintenance Model Data/'

mains_repairs_file = '02 Mains Repairs EDA v0 1.csv'
ancillary_repairs_file = '03 Ancillary Repairs v1 1516.csv'
interruptions_file = '05 Interruptions EDA v0.csv'

# In[]

def date_parser(d):
    if pd.isnull(d):
        return pd.NaT
    try:
        return pd.to_datetime(d, format = "%d/%m/%Y")
    except:
        return pd.NaT      

def datetime_parser(d,t):
    try:
        return pd.to_datetime(d + " " + t, format = "%d/%m/%Y %H:%M:%S")
    except:
        return pd.NaT

def read_repairs(infile):
    MainsRepairsDF = pd.read_csv(path + infile,
                                 parse_dates = ['Notification Date'],
                                 date_parser = date_parser,
                                 encoding = 'latin-1')
    MainsRepairsDF['Notification Date'] = MainsRepairsDF['Notification Date'].dt.date
    MainsRepairsDF['DMA'] = MainsRepairsDF['Floc. DMA_ID'].apply(fix_floc_dma_id)
    MainsRepairsDF.rename(columns = {'Operation':'OrderAndOp'}, inplace = True)
    MainsRepairsDF['Order'] = MainsRepairsDF['OrderAndOp'].apply(lambda s: s.split("/")[0])
    MainsRepairsDF['Operation'] = MainsRepairsDF['OrderAndOp'].apply(lambda s: s.split("/")[1])
    MainsRepairsDF['SourceFile'] = infile
    return MainsRepairsDF
    
def read_interruptions(infile):
    InterruptionsDF = pd.read_csv(path + infile,
                                  parse_dates = {'EndDateTime': ['End Date', 'End Time'],
                                                 'StartDateTime': ['Start Date', 'Start Time']},
                                  date_parser = datetime_parser,
                                  encoding = 'latin-1')
    InterruptionsDF.rename(columns = {'DMA(s)':'DMA', 'Order Number':'Order'}, inplace = True)
    InterruptionsDF['SourceFile'] = infile
    return InterruptionsDF

InterruptionsDF = read_interruptions(interruptions_file)
MainsRepairsDF = read_repairs(mains_repairs_file)
AncillaryRepairsDF = read_repairs(ancillary_repairs_file)    








