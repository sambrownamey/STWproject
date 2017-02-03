# -*- coding: utf-8 -*-
"""
This script reads the planned and unplanned work extracts (from Darren and 
Amy respectively) into dataframes

@author: 142796
"""

import pandas as pd
import os

path = ("S:/00_Projects/CO00690090_STW WQ Analytics/"
        "04_Background_Documents_Data/00. MI Analysis/"
        "Data/Planned-Unplanned Work/")

picklepathplanned = 'PickledData/SAPPlannedDF.pkl'
picklepathunplanned = 'PickledData/SAPUnplannedDF.pkl'

#%% Planned work

# Note this file has no completion date field
def planned():
    infile = 'Planned Work_Water Quality hypotheses.csv'
    
    SAPPlannedDF = pd.read_csv(path + infile,
                               encoding = 'latin-1',
                               parse_dates = ['Op. Created date'],
                               dayfirst = True,
                               na_values = ['Not assigned', '#', '#/#'])
    
    def crop_dma(s):
        if pd.notnull(s):
            return s[:5]
        else:
            return pd.np.nan
            
    SAPPlannedDF['DMA'] = SAPPlannedDF['Floc. DMA_ID'].apply(crop_dma)
    
    # Remove stray waste water works
    
    waste =['Sewage Treatment - West', 
            'Waste Water - East', 
            'Waste Water - South',
            'Waste Water - West']
            
    SAPPlannedDF = SAPPlannedDF[
            ~(SAPPlannedDF['Ord. Maintenance Plant'].isin(waste))]
    SAPPlannedDF.to_pickle(picklepathplanned)

#%% 

def unplanned():
    years = [str(y) for y in range(2010, 2017)]
    fileroot = 'All Unplanned Ops '
    
    dataframes = {}
    
    # Read in the data (different numbers of sheets for different years)
    
    for y in years:
        if y == '2010':
            sheets = list(range(7))
        elif y in ['2011', '2012']:
            sheets = list(range(12))
        else:
            sheets = [0]
        dataframes[y] = pd.read_excel(path + fileroot + y + ".xlsx",
                                      sheetname = sheets,
                                      na_values = ['Not assigned', '#', '#/#'])   
    
    # Flatten into a single df    
        
    flattened_dfs = []
    
    for y in years:
        for key in dataframes[y].keys():
            flattened_dfs.append(dataframes[y][key])
    
    SAPUnplannedDF = flattened_dfs[0]
    
    for df in flattened_dfs[1:]:
        SAPUnplannedDF = SAPUnplannedDF.append(
                             df,
                             ignore_index = True)
    
    # Fix the DMA field
        
    SAPUnplannedDF['DMA'] = SAPUnplannedDF['Floc. DMA_ID'].apply(crop_dma)
    SAPUnplannedDF.to_pickle(picklepathunplanned)

if __name__ == "__main__":
    planned()
    unplanned()
    
SAPPlannedDF = pd.read_pickle(picklepathplanned)
SAPUnplannedDF = pd.read_pickle(picklepathunplanned)
    



