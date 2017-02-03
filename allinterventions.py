# -*- coding: utf-8 -*-
"""
This script pulls together a dataframe of everything that happens.

@author: 142796
"""

import pandas as pd

# Valve operations and GISST burst main data
from stwpy.gisst_sap_data_reader import GVODF, valve_op_datecols, GBMDF, burst_main_datecols

# Burst mains. We use mains repairs data from Jon Hagan's model, for now.
from stwpy.inframodel_reader import MainsRepairsDF, InterruptionsDF#, AncillaryRepairsDF

# Import bursts (Netbase)
from stwpy.bursts_repairs_reader import BurstsGroupedWithDMA

# SAP planned/unplanned
from stwpy.sap_planned_unplanned_reader import SAPPlannedDF, SAPUnplannedDF

# Network control data
from stwpy.nc_data_reader import IncidentsGrouped, NCPlannedDF
from stwpy.dmautils import DMA_NAME_WQZ_CG_SR

# Change pickle to True to pickle the data after collating
pickle = False

#%% Ancillary functions

# Sort out the valve ops/BMs data

def process_gisst(DF, datecols):
    DF['LatestDate'] = DF[datecols].idxmax(axis = 1)
    
    # Filter for completed operations only
    
    DF['CompletedOp'] = DF['DATE_COMPLETED'].notnull()
    DF = DF[DF['CompletedOp']]
    
    # Infer a start date/time for the operation
    
    DF['InferredStartDate'] = DF[[
     'DATE_CREATED', 
     'DATE_DISPATCHED',
     'DATE_ACCEPTED',
     'DATE_ARRIVED']].apply(
         lambda row: max([x for x in row if pd.notnull(x)]) 
                      if row.notnull().any() 
                      else pd.NaT, 
         axis=1
         )
    
    # Infer a finish date/time for the operation
    
    try: 
        DF['InferredFinishDate'] = DF['OP_ACTUAL_FINISH_DATE']
        DF['InferredFinishDate'] = DF['InferredFinishDate'].fillna(
            DF['DATE_COMPLETED'])
    except KeyError:
        DF['InferredFinishDate'] = DF['DATE_COMPLETED']
        
    return DF

def x_from_xy(s):
    if s == 'Not assigned':
        return pd.np.nan
    else:
        try:
            return str(s)[:6]
        except:
            return pd.np.nan

def y_from_xy(s):
    if s == 'Not assigned':
        return pd.np.nan
    else:
        try:
            return str(s)[-6:]
        except:
            return pd.np.nan        

def try_get_order(s):
    if pd.isnull(s):
        return pd.np.nan
    else:
        try:
            return int(s)
        except:
            try:
                repd = s.replace(" ","/").replace("&","/").replace("-","/")
                split = repd.split("/")
                try:
                    return int(split[0])
                except:
                    return int(split[1])
            except:
                return s

#%% Further data processing (mainly in scripts alone)
                     
BurstsGroupedWithDMA['DateType'] = 'Burst reported date'
BurstsGroupedWithDMA['SourceFile'] = 'BurstsNetbase.csv'
BurstsGroupedWithDMA.rename(columns = {'Burst Type':'InterventionType',
                                       'Date Reported':'StartDateTime'},
                                       inplace = True) 
BurstsGroupedWithDMA['Operation'] = 0

# TODO: put in the end date time of the burst, if we can attribute complaints
# during the burst or up to 24h afterwards. Question: how do we know when 
# the burst end? Will this catch too many complaints?
                  
MainsRepairsDF['DateType'] = 'SAP Notification Date'
MainsRepairsDF.rename(columns = {#'Notification Date':'StartDateTime',
                                 'Nearest Neighbour Pipe ID':'AssetID'},
                      inplace = True)
MainsRepairsDF['Description'] = MainsRepairsDF[['FDB Technique', 
                                                'FDB Size',
                                                'FDB Material']].apply(
                                                    ", ".join,
                                                    axis = 1)

GBMProcessed = process_gisst(GBMDF, burst_main_datecols)

valve_op_datecols.remove('VALVE_STATUS_DATE')
VOProcessed = process_gisst(GVODF, valve_op_datecols)

MergedMR = pd.merge(left = MainsRepairsDF, 
                    right = GBMProcessed, 
                    on = 'OrderAndOp', 
                    how = 'left', 
                    indicator = 'Merged')  

MergedMR.rename(columns={'InferredStartDate': 'StartDateTime',
                         'InferredFinishDate': 'EndDateTime',
                         'DMA_x':'DMA'},
                inplace=True)

                                              
# Not including Ancillary repairs                                                
#AncillaryRepairsDF['DateType'] = 'SAP Notification Date'                     
#AncillaryRepairsDF.rename(columns = {'Notification Date':'StartDateTime',
#                                     'Operation Description':'Description'},
#                          inplace = True)
#AncillaryRepairsDF['X'] = AncillaryRepairsDF['Ord. Address XY'].apply(x_from_xy)      
#AncillaryRepairsDF['Y'] = AncillaryRepairsDF['Ord. Address XY'].apply(y_from_xy)                 
# 
                 
InterruptionsDF['DateType'] = 'Start and end datetimes'
InterruptionsDF['Description'] = 'Interruption, cause: ' + InterruptionsDF['Cause of Interruption']
InterruptionsDF.rename(columns = {'Grid Ref X':'X',
                                  'Grid Ref Y':'Y'},
                       inplace = True)


SAPPlannedDF['DateType'] = 'SAP Operation Created Date'

#Filter for only water distribution work (i.e. infra)

Infra = ['Water Distribution - East',
         'Water Distribution - South',
         'Water Distribution - West']

SAPPlannedDF = SAPPlannedDF[SAPPlannedDF['Ord. Maintenance Plant'].isin(Infra)]
SAPPlannedDF.rename(columns = {'Op. Created date':'StartDateTime',
                               'Unnamed: 12':'Description'},
                    inplace = True)

SAPPlannedDF['X'] = SAPPlannedDF['X-Y Co-ordinates'].apply(x_from_xy)
SAPPlannedDF['Y'] = SAPPlannedDF['X-Y Co-ordinates'].apply(y_from_xy)                   

SAPUnplannedDF['DateType'] = 'SAP Operation Created and Completed Dates'

#Filter for infra work only
SAPUnplannedDF = SAPUnplannedDF[SAPUnplannedDF['Unnamed: 3'].isin(Infra)]

SAPUnplannedDF.rename(columns = {'Op. Created date':'StartDateTime',
                                 'Op. Completed date':'EndDateTime',
                                 'Unnamed: 15':'Description'},
                      inplace = True)  

SAPUnplannedDF['X'] = SAPUnplannedDF['X-Y Co-ordinates'].apply(x_from_xy)
SAPUnplannedDF['Y'] = SAPUnplannedDF['X-Y Co-ordinates'].apply(y_from_xy)                   

IncidentsGrouped = IncidentsGrouped.reset_index()                      
IncidentsGrouped['DateType'] = 'Op created and latest task on NC incident tracker'
IncidentsGrouped['Description'] = 'Incident: ' + IncidentsGrouped['IncidentName']
IncidentsGrouped['Order'].replace('0', pd.np.nan, inplace=True)
IncidentsGrouped['Order'].fillna('NoOrder'+IncidentsGrouped['IncidentName'], inplace=True)
                    
NCPlannedDF['DateType'] = 'Planned Start/End from NC Planned work sheet'
NCPlannedDF.rename(columns = {'SAP Number':'Order'},
                   inplace = True)
NCPlannedDF['Description'] = NCPlannedDF['Work type'] + ": " + NCPlannedDF['Description']

VOProcessed['DateType'] = 'Inferred from different SAP date fields'

VOProcessed.rename(columns={'UADMS_ID': 'AssetID',
                      'STD_TEXT_KEY_DESC': 'Description',
                      'POINT_X': 'X',
                      'POINT_Y': 'Y'},
             inplace=True)

VOProcessed.rename(columns={'WORK_ORDER': 'Order',
                      'InferredStartDate': 'StartDateTime',
                      'InferredFinishDate': 'EndDateTime',
                      'OPERATION_NUMBER': 'Operation'},
             inplace=True)

df_types = {'Mains Repair': MergedMR,
            #'Ancillary Repair': AncillaryRepairsDF,
            'Interruption': InterruptionsDF,
            'SAP Planned work': SAPPlannedDF,
            'SAP Unplanned work': SAPUnplannedDF,
            'NC Incident': IncidentsGrouped,
            'NC Planned work': NCPlannedDF,
            'Valve Op': VOProcessed}

for intervention_type in df_types.keys():
    df_types[intervention_type]['InterventionType'] = intervention_type

#%% Make the dataframe

cols_for_directory = ['StartDateTime',
                      'EndDateTime',
                      'DateType',
                      'DMA',
                      'ControlGroup',
                      'SAPRegion',
                      'Order',
                      'Operation',
                      'InterventionType',
                      'Description',
                      'AssetID',
                      'X',
                      'Y',
                      'SourceFile',
                      'Key']

dataframe = pd.DataFrame()

for df in list(df_types.values()) + [BurstsGroupedWithDMA]:
    df['Key'] = df.index
    cols = [c for c in cols_for_directory if c in df.columns]
    dataframe = dataframe.append(df[cols], ignore_index=True)

nullsd = dataframe[dataframe['StartDateTime'].isnull()]

dataframe['StartDateTime'] = pd.to_datetime(
    dataframe['StartDateTime'],
    errors='coerce',
    dayfirst=True)

dataframe['EndDateTime'] = pd.to_datetime(
    dataframe['EndDateTime'],
    errors='coerce',
    dayfirst=True)

dataframe = dataframe[cols_for_directory]

dataframe = pd.merge(dataframe,
                     DMA_NAME_WQZ_CG_SR,
                     how='left',
                     left_on='DMA',
                     right_index=True,
                     suffixes=('', 'FromDMA'))

dataframe['ControlGroup'].fillna(
    dataframe['ControlGroupFromDMA'],
    inplace=True)

dataframe['SAPRegion'].fillna(
    dataframe['SAPRegionFromDMA'],
    inplace=True)

dataframe['OrderOrig'] = dataframe['Order']
dataframe['Order'] = dataframe['Order'].apply(try_get_order)
dataframe['OpOrig'] = dataframe['Operation']
dataframe['Operation'] = dataframe['Operation'].apply(try_get_order)
dataframe['Operation'] = dataframe['Operation'].fillna(0)
dataframe['Date'] = dataframe['StartDateTime'].dt.date
dataframe['Time'] = dataframe['StartDateTime'].dt.time
dataframe['StartDateTime'] = dataframe['StartDateTime'].fillna(
        dataframe['EndDateTime'] - pd.Timedelta('1 hour'))
dataframe['EndDateTime'] = dataframe['EndDateTime'].fillna(
        dataframe['StartDateTime'] + pd.Timedelta('1 hour'))
dataframe['DMA'] = dataframe['DMA'].astype(str)
dataframe['ControlGroup'] = dataframe['ControlGroup'].astype(str)
dataframe['WQZ'] = dataframe['WQZ'].astype(str)

# Get all the events with a defined SAP region (for basic filtering)

localevents = dataframe[dataframe['SAPRegion'].notnull()]
localevents = localevents.dropna(subset = ['StartDateTime', 'EndDateTime'])                        

#%%

fullevents = localevents.groupby(
    by = ['Order', 'Operation', 'InterventionType', 'DMA', 'WQZ', 'ControlGroup', 'SAPRegion']).agg({
        'StartDateTime':{'Start':min},
        'EndDateTime':{'End':max}
        #'SAPRegion':{'SAPRegion':min}
        })

fullevents.columns = fullevents.columns.droplevel(0)
fullevents = pd.DataFrame(fullevents).reset_index()

fullevents['Year'] = fullevents['Start'].dt.year
fullevents['Month'] = fullevents['Start'].dt.month
fullevents['StartDate'] = fullevents['Start'].dt.date
fullevents['EndDate'] = fullevents['End'].dt.date

fullevents.groupby(by = ['Year', 'SAPRegion', 'InterventionType']).size()

if pickle:
    fullevents.to_pickle('PickledData/FullEventsList.pkl')
                                              
#%% #Filter for first quarter of 2016 in four WQZs
events2016firstquarter = localevents[
        (localevents['StartDateTime'] >= pd.to_datetime("2016-01-01")) &
        (localevents['StartDateTime'] <= pd.to_datetime("2016-04-01"))]

gpdtypes2016janmar = events2016firstquarter.groupby(
    by = ['Order', 'Operation', 'InterventionType', 'WQZ']).agg({
        'StartDateTime':{'Start':min},
        'EndDateTime':{'End':max},
        'InterventionType':{'Type':min},
        'DMA':{'DMA':min},
        #'SAPRegion':{'SAPRegion':min}
        })

gpdtypes2016janmar.columns = gpdtypes2016janmar.columns.droplevel(0)
gpdtypes2016janmar = pd.DataFrame(gpdtypes2016janmar).reset_index()

WQZfilter = ['Bordesley', 'Selly Park', 'Solihull', 'Hagley']

filtered = gpdtypes2016janmar[gpdtypes2016janmar['WQZ'].isin(WQZfilter)]
