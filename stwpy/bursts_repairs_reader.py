# -*- coding: utf-8 -*-
"""
This script reads in the data from the "demand and leakage" folder, which
includes bursts and repairs (think it's more extensive than Jon Hagan's data).
It then uses the full SAP work order extract from GISST to get better info
about the times for the notification (i.e. the burst) and the repair work
itself.

@author: 142796
"""

import pandas as pd
import os
from stwpy.dmautils import fix_floc_dma_id as fixdma

rootpath = 'S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/00. MI Analysis/Data/Demand and Leakage/'

subfolders = ['Completed Bopps/',
              'Completed Comms Ancilliary/',
              'Completed Mains/',
              'Raised BOPPs/',
              'Raised Leaks Exc Bopps/']
              
# Things in completed are also in raised (or so it looks!)

paths = [rootpath + s for s in subfolders]

# In[]

def date_parser(d):
    if pd.isnull(d):
        return pd.NaT
    try:
        return pd.to_datetime(d, format = "%d/%m/%Y")
    except:
        return pd.NaT      


def read_spreadsheet(path, file):
    df = pd.read_excel(path + file, 
                       sheetname = 1, 
                       skiprows = 14, 
                       parse_cols = "G:AE")
    df = df.dropna(axis = 0, how = 'all')
    df = df.dropna(axis = 1, how = 'all')
    #print("Shape:", df.shape)
    #print("Columns:", df.columns)
    return df    
        
def read_data():
    dataframes = []
    for path in paths:
        dfbig = pd.DataFrame()
        for file in os.listdir(path):
            print("Trying to read", file)
            try:
                df = read_spreadsheet(path, file)
                df['SourceFile'] = file
                dfbig = dfbig.append(df, ignore_index = True)
                print("Successfully read", file)
            except:
                print("Error reading file", file, "continuing.")
        dataframes.append(dfbig)
    return dataframes


        
        
def read_repairs(infile):
    MainsRepairsDF = pd.read_csv(path + infile,
                                 parse_dates = ['Notification Date'],
                                 date_parser = date_parser,
                                 encoding = 'latin-1')
    MainsRepairsDF['Notification Date'] = MainsRepairsDF['Notification Date'].dt.date
    MainsRepairsDF['DMA'] = MainsRepairsDF['Floc. DMA_ID'].apply(fixdma)
    MainsRepairsDF.rename(columns = {'Operation':'OrderAndOp'}, inplace = True)
    MainsRepairsDF['Order'] = MainsRepairsDF['OrderAndOp'].apply(lambda s: s.split("/")[0])
    MainsRepairsDF['Operation'] = MainsRepairsDF['OrderAndOp'].apply(lambda s: s.split("/")[1])
    MainsRepairsDF['SourceFile'] = infile
    return MainsRepairsDF
    

    

# In[]
if __name__=='__main__':
    dfs = read_data()
    for i in [0,2]:
        dfs[i]['X'] = dfs[i].apply(
            lambda row: pd.np.nan if row['Ord. Address XY']=='Not assigned'
                        else row['Ord. Address XY'][:6],
            axis = 1)
        dfs[i]['Y'] = dfs[i].apply(
            lambda row: pd.np.nan if row['Ord. Address XY']=='Not assigned'
                        else row['Ord. Address XY'][-6:],
            axis = 1)
        dfs[i].drop(['Ord. Address XY', 'Area'], axis = 'columns', inplace = True)
    
    dfs[1].rename(columns = {'FWR Grid Ref X': 'X',
                             'FWR Grid Ref Y': 'Y',
                             'Unnamed: 23': 'Unnamed: 22'},
                  inplace = True)
    
    CompletedWorksDF = dfs[0].append([dfs[1], dfs[2]], ignore_index=True)
    CompletedWorksDF.to_pickle('PickledData/CompletedWorksDFs')
else:
    CompletedWorksDF = pd.read_pickle('PickledData/CompletedWorksDFs')
# Columns are the same for BOPPS and mains, but differ for Comms/Ancillary
# as X and Y are separated. 

#%% Parse the dates to datetimes

for datecol in ['Op. Actual Finish date', 'Op. Created date']:
    CompletedWorksDF[datecol] = CompletedWorksDF[datecol].apply(
        date_parser).dt.date

CompletedWorksDF.rename(columns = {'Unnamed: 22': 'Notn Description'},
                        inplace = True)

#%%

def order_and_op(row):
    try:
        return "/".join([str(int(row.Order)), str(int(row.Operation))])
    except:
        return pd.np.nan

CompletedWorksDF['OrderAndOp'] = CompletedWorksDF.apply(order_and_op,
                                                        axis = 1)

#%% Fix DMA

CompletedWorksDF['DMA'] = CompletedWorksDF['Floc. DMA_ID'].apply(fixdma)

# TODO: Link to raised works
#%%

CompletedWorksGpd = pd.DataFrame(
    CompletedWorksDF.groupby(by = ['Order', 'DMA'], as_index = False).size()
    ).reset_index()

CompletedWorksGpd = CompletedWorksGpd[['Order', 'DMA']]

from stwpy.netbase_bursts_reader import BurstsNetbaseDF, BurstsGrouped

#%% Read the big SAP workorder list and merge
if __name__=='__main__':    
    infile = "../../04_Background_Documents_Data/SAP_Work_Order_Summary.csv"
    
    def datetime_parser(dt):
        try:
            return pd.to_datetime(dt, format = "%d/%m/%Y %H:%M:%S")
        except:
            return pd.NaT
    
    datecols = ['CUR_OP_STATUS_DATE',
                'CUR_OP_EARLY_START_DATE',
                'CUR_OP_LATEST_FINISH_DATE',
              # 'CUR_OP_ACTUAL_FINISH_DATE', This column is all nans
                'WO_EARLIEST_START_DATE',
                'WO_LATEST_FINISH_DATE',
                'CREATED_DATE',
                'MODIFIED_DATE']
    
    df = pd.read_csv(infile,
                     parse_dates = datecols,
                     date_parser = datetime_parser,
                     thousands = ',')
    
    SAPWOSummaryDF = df
    
    # This SAP extract doesn't go back as far as we'd hoped... earliest created
    # date is 2015-03-02
    
    # Hypothesis: it contains only work orders with an op created since then...
    
    # Coverage is OK for recent dates when joining on order and operation. Maybe
    # this misses out workorders with another, more recent, operation...?
    
    # TODO: import this into the allinterventions.py script, using the new dates
    # to get more informed dates for bursts and mains repairs separately.

    #%%
    
    def try_year(d):
        try:
            return d.year
        except:
            return pd.NaT
    
    CompletedWorksDF['Year'] = CompletedWorksDF['Op. Created date'].apply(try_year)
    
    
    MergeA = pd.merge(CompletedWorksDF,
                      BurstsNetbaseDF,
                      how = 'outer',
                      on = 'OrderAndOp',
                      indicator = True)
    
    
    MergeA.groupby(by = ['ESPB Type', '_merge']).size()
    
    MergeA.groupby(by = ['_merge']).size()
    
    # All the mains/comms repairs in the leakage data also appear on 
    # BurstsNetbaseDF, along with the time that the burst was reported! About half
    # of the ancillary repairs also appear. Maybe these are only those that are
    # associated with bursts...?
    
    from stwpy.inframodel_reader import MainsRepairsDF, AncillaryRepairsDF
    
    MergeB = pd.merge(MainsRepairsDF,
                      MergeA,
                      how = 'outer',
                      on = 'OrderAndOp',
                      indicator = 'MergedB')
    
    MergeC = pd.merge(AncillaryRepairsDF,
                      MergeA,
                      how = 'outer',
                      on = 'OrderAndOp',
                      indicator = 'MergedC')
    
    # All but 47 of the mains repairs in MainsRepairsDF are also in BurstsNetbaseDF
    

from stwpy.dmautils import pctodma

BurstsGroupedWithDMA = pd.merge(BurstsGrouped, 
                                CompletedWorksGpd, 
                                how='left',
                                on='Order')

def try_dma_from_pc(pc):
    try:
        return pctodma[pc].zfill(5)
    except:
        return pd.np.nan
        

BurstsGroupedWithDMA['DMAfromPC'] = BurstsGroupedWithDMA['Postcode'].apply(
    try_dma_from_pc)

BurstsGroupedWithDMA['DMA'].fillna(BurstsGroupedWithDMA['DMAfromPC'],
                                   inplace=True)






