# -*- coding: utf-8 -*-
"""
This contains utility functions related to DMAs and associated geographical
levels for the STW area.

@author: 142796
"""

import pandas as pd

DMAsNetbasePath = 'linking_data/DMAsNetbase.csv'
dma_neighbours = 'linking_data/dma_neighbours.csv'
postcode_dma_wqz_2016 = 'linking_data/Postcode_DMA_WQZ_2016.csv'


DMAsNetbase = pd.read_csv(DMAsNetbasePath, 
                          encoding = 'latin-1', 
                          dtype = {'Reference':str} )

TM_DMAs = DMAsNetbase[DMAsNetbase['TMA']=='Yes'][['Name', 'Reference']].set_index('Name')

DMAtoSapRegion = DMAsNetbase[['SAP Area - Description', 'Reference']]

DMA_NAME_CG_SR = DMAsNetbase[['Reference', 'Name', 'Control Group - Description', 'SAP Area - Description']]
DMA_NAME_CG_SR.index = DMA_NAME_CG_SR['Reference']
DMA_NAME_CG_SR = DMA_NAME_CG_SR.drop('Reference', axis = 'columns')
DMA_NAME_CG_SR.columns = ['DMAName', 'ControlGroup', 'SAPRegion']

DMA_NAME_WQZ_CG_SR = DMAsNetbase[['Reference', 'Name', 'Water Quality Zone - Description', 'Control Group - Description', 'SAP Area - Description']]
DMA_NAME_WQZ_CG_SR.index = DMA_NAME_WQZ_CG_SR['Reference']
DMA_NAME_WQZ_CG_SR = DMA_NAME_WQZ_CG_SR.drop('Reference', axis = 'columns')
DMA_NAME_WQZ_CG_SR.columns = ['DMAName', 'WQZ', 'ControlGroup', 'SAPRegion']


def list_dmas_in_region(reg):
    return list(DMAtoSapRegion[DMAtoSapRegion['SAP Area - Description']==reg]['Reference'].values)

SapAreas = ['Central', 'Derbyshire', 'Leicestershire', 'Nottinghamshire',
       'Shropshire', 'Staffordshire', 'Warwickshire', 'Worcs and Gloucs']
       
def fix_floc_dma_id(s):
    """ 
    Fixes the 'Floc. DMA_ID' field found in some SAP extracts
    """
    if pd.isnull(s):
        return pd.np.nan
    else:
        try:
            prefix = s[:5]
            if prefix.isdigit():
                return prefix
            else:
                try:
                    return TM_DMAs.ix[s][0]
                except KeyError:
                    return pd.np.nan
        except:
            return pd.np.nan

def fix_dma(s):
    """
    Converts DMA to five-digit string e.g. "04722". Should work if the input is 
    a 4 or 5 digit string, an integer, or a float. Will return None on error.
    """
    try:
        if type(s) == float:
            s = int(s)
        return str(s.strip()).zfill(5)
    except:
        return None

           
DMAneighbours = pd.read_csv(dma_neighbours)
DMAneighbours["NEIGHBORS"] = DMAneighbours["NEIGHBORS"].astype(str).apply(lambda s: s.split(","))
DMAneighbours = DMAneighbours.set_index('ID_DMA_COD')
DMAneighbours = pd.Series(DMAneighbours['NEIGHBORS'])

link = pd.read_csv(postcode_dma_wqz_2016)

#Remove spaces
def trim(string):
    try:
        return string.replace(" ","")
    except:
        return None

#Tidying the link dataframe        
link['Postcode'] = link['Postcode'].map(trim)     

#Creating two dictionaries
pctodma = link.set_index('Postcode')['DMA'].to_dict()














       