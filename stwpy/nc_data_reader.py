# -*- coding: utf-8 -*-
"""
This script reads data from the Network Control incident tracker and planned
work sheet.

@author: 142796
"""

import pandas as pd

#%% Incident tracker

path = "S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/IncidentTracker/"
infile = "MergedIncidentTrackerWithoutBadLines.csv"

IncidentsDF = pd.read_csv(path + infile, 
                          encoding = "latin-1",
                          parse_dates = ['Created Date', 'Timestamp'],
                          keep_date_col = True,
                          dayfirst = True,
                          index_col = 0)

IncidentsDF['SourceFile'] = infile
IncidentsDF.rename(columns = {'Created Date':'StartDateTime'}, inplace = True)
IncidentsDF['StartDate'] = IncidentsDF['StartDateTime'].dt.date

#%%

# Dictionary for fixing the manually entered DMAs. Potential task for later:
# add support for multiple DMAs for one incident (as in commented out lines)

dma_exceptions = {'0': None,
  '000': None,
  #'07/320 - 07/318 - 07/322': ['07320', '07318', '07322'],
  '07/320 - 07/318 - 07/322': '07320',
  #'10/028 501 533 530 534 524 535 542': ['10028', '10501', '10533', '10530', '10534', '10524', '10535', '10542'],
  '10/028 501 533 530 534 524 535 542': '10028',
  #'12207 12208 12230': ['12207', '12208', '12230'],
  '12207 12208 12230': '12207',
  '6/519 (Multiple)': '06519',
  #'8/605 & 8/601': ['08605', '08601'],
  '8/605 & 8/601': '08605',
  'All - Strelley c/g': 'Multiple',
  'Several': 'Multiple',
  'drainage area': None,
  'n/a': None,
  'many': 'Multiple'}

def fix_dma(s):
    if pd.isnull(s):
        return None
    elif s in dma_exceptions.keys():
        return dma_exceptions[s]
    else:
        try:
            s = str(s)
            sr = s.replace("/","")
            sr = sr.replace(" ","")
            sr = sr.replace("*","")
            return sr.zfill(5)
        except:
            return None

#%%

IncidentsDF['DMA'] = IncidentsDF['Dma'].apply(fix_dma)

IncidentsDF['IncidentName'] = IncidentsDF[['Name', 'StartDate']].apply(
    lambda x: '_'.join([x[0], 
                        str(x[1])]
                      ),
    axis = 1
    )

    
# Make region names consistent
IncidentsDF['Region'].replace('Worc/Gloucs', 'Worcs and Gloucs', inplace = True)    
    
    
# Fill in blank timestamps with date created

IncidentsDF['Timestamp'].fillna(IncidentsDF['StartDateTime'], inplace = True)

    
    
grouped = IncidentsDF.groupby(by = 'IncidentName')

IncidentsGrouped = grouped.agg({'Timestamp':{'StartDateTime':min, 'EndDateTime':max},
                                'DMA':{'DMA':min}, 
                                'Sap Order':{'Order':min, 'NumRecords':len},
                                'X':{'X':pd.np.median},
                                'Y':{'Y':pd.np.median},
                                'Region':{'SAPRegion':min}})

IncidentsGrouped.columns = IncidentsGrouped.columns.droplevel(0)
IncidentsGrouped['SourceFile'] = infile + " (grouped)"

#Filter out the training events

def cont_training(string):
    return "training" in string.lower() or "NETWORK CONTROL TEST INCIDENT" in string.lower()

IncidentsGrouped=IncidentsGrouped.reset_index() 
    
IncidentsGrouped['Training'] = IncidentsGrouped['IncidentName'].apply(
    cont_training)

IncidentsGrouped = IncidentsGrouped[IncidentsGrouped['Training']==False]


#%% PLANNED WORK

plannedworkpath = 'S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/00. MI Analysis/Data/Network Control/'
oldfile = "New 2016 Planned Works.xlsm"
newfile = "NCPlannedWork030117.csv"

pwdfdict = pd.read_excel(plannedworkpath + oldfile, 
                         sheetname = [0,1,2,3,4,5,6], 
                         skiprows = 2, 
                         header = 0,
                         parse_cols = "D:Q")

# Verify columns are the same for the seven sheets

for i in range(7):
    print((pwdfdict[i].columns == pwdfdict[0].columns).all())
    
# Concatenate

NCPlannedDF = pwdfdict[0]
for i in range(1,7):
    NCPlannedDF = NCPlannedDF.append(pwdfdict[i], ignore_index = True)

# Drop blank rows
    
NCPlannedDF = NCPlannedDF.dropna(axis = 0, how = 'all')

# Sort out the start and end timestamps

NCPlannedDF['Start Date'] = NCPlannedDF['Start Date'].dt.date
NCPlannedDF['End Date'] = NCPlannedDF['End Date'].dt.date

NCPlannedDF['SourceFile'] = oldfile

def time_parser(t, end = False):
    if end == False:
        default = pd.datetime(1900, 1, 1).time() # midnight
    else:
        default = pd.datetime(1900, 1, 1, 23, 59, 59).time() # just before midnight
    night = pd.datetime(1900, 1, 1, 22, 0, 0).time()
    try:
        return pd.to_datetime(str(t)).time()
    except:
        try:
            if "night" in t.lower():
                return night
            else:
                return default
        except:
            return default
            
def time_combiner(r, datecol, timecol):
    try:
        return pd.datetime.combine(r[datecol], r[timecol])
    except:
        return pd.NaT
        
NCPlannedDF['CleanStartTime'] = NCPlannedDF['Start Time'].apply(
    lambda t: time_parser(t, end = False))
NCPlannedDF['CleanEndTime'] = NCPlannedDF['End Time'].apply(
    lambda t:time_parser(t, end = True))

NCPlannedDF['StartDateTime'] = NCPlannedDF.apply(
    lambda r: time_combiner(r, 'Start Date', 'CleanStartTime'),1)
NCPlannedDF['EndDateTime'] = NCPlannedDF.apply(
    lambda r: time_combiner(r, 'End Date', 'CleanEndTime'),1)

# Fix the dma for district/dma syntax

# TODO: add support here for multiple DMAs (e.g. every DMA in a control group)

def fix_dma(county, dma3):
    try:
        county = str(int(county))
        dma3 = str(int(dma3)).zfill(3)
        dma = county + dma3
        dma = dma.zfill(5)
        return dma
    except:
        return pd.np.nan
        
NCPlannedDF['DMA'] = NCPlannedDF.apply(
    lambda row: fix_dma(row['County'], row['DMA(s)']), 1)
        
NCPlannedDF.drop(['DMA(s)', 
                  'CleanStartTime', 
                  'CleanEndTime',
                  'Start Date',
                  'End Date'], 
                  axis = 1, 
                  inplace = True)

OldToNewColumnsDict = {
    'Type':'Work type',
    'County':'District',
    'BB Notification':'SAP Number',
    'Contact':'Contact Name',
    'COSC Auth.':'Authorised By', 
    'StartDateTime':'Start Date/Time',
    'Start Time':'RawStartTime',
    'End Time':'RawEndTime', 
    'EndDateTime':'End Date/Time'
        }


NCPlannedDF.rename(columns = OldToNewColumnsDict, inplace = True) 

NCNewPlannedDF = pd.read_csv(plannedworkpath + newfile, 
                 encoding = 'latin-1',
                 parse_dates = ['Start Date/Time', 'End Date/Time'],
                 dayfirst = True)

NCNewPlannedDF['SourceFile'] = newfile
NCNewPlannedDF['DMA'] = NCNewPlannedDF.apply(
    lambda row: fix_dma(row['District'], row['DMA']), 1)

NCNewPlannedDF.drop(['Item Type', 'Path'], axis = 1, inplace = True)

NCPlannedDF = NCPlannedDF.append(NCNewPlannedDF, ignore_index = True)

cols = ['Work type', 'District', 'DMA', 'Region', 'Control Group', 'SAP Number',
       'Works Name', 'Description', 'Start Date/Time', 'End Date/Time',
       'RawStartTime', 'RawEndTime', 'Contact Name', 'Contact Number', 
       'Authorised By', 'Enabling work?', 'Customer warning?', 'SourceFile']

    
NCPlannedDF = NCPlannedDF[cols]

NCPlannedDF.rename(columns = {'Start Date/Time':'StartDateTime', 
                              'End Date/Time':'EndDateTime'},
                   inplace = True) 

# Manual data cleaning: work type is not always right

NCPlannedDF.replace({'Work type':{'Rehab':'Infra',
                                  'Works':'Non-Infra',
                                  'LD':'Leak Detection',
                                  'infra':'Infra',
                                  'rehab':'Infra',
                                  60197388:'Infra',
                                  356906:'Infra',
                                  36288:'Infra',
                                  'Please ensure wholesales ops (Noel Hughes) is fully informed the proposed commission work.':'Infra'}},
                                  inplace = True)

# SAP region is all over the place

from stwpy.dmautils import SapAreas, DMA_NAME_CG_SR

NCPlannedDF = NCPlannedDF.join(DMA_NAME_CG_SR, on = 'DMA')

# Only need to change those without a DMA:

#nulldma = NCPlannedDF[NCPlannedDF['DMA'].isnull()]
#nulldma.groupby(by = 'Region').size().index

NCPlannedDF['OrigReg'] = NCPlannedDF['Region']

sapregdict = {'Birmingham':'Central', 
              'Gloucester': 'Worcs and Gloucs',
              'Gloucestershire': 'Worcs and Gloucs',
              'Gloucstershire': 'Worcs and Gloucs', 
              'Nottingham':'Nottinghamshire',
              'Shelton':'Staffordshire', 
              'Worcester':'Worcs and Gloucs',
              'Worcs/Glos':'Worcs and Gloucs',
              'Worc/Gloucs':'Worcs and Gloucs',
              'Worcester / Gloucester':'Worcs and Gloucs',
              'Worcs/Gloucs':'Worcs and Gloucs', 
              'Worksop':'Nottinghamshire', 
              'central':'Central', 
              'derbyshire':'Derbyshire',
              'nottingham':'Nottinghamshire', 
              'nottinghamshire':'Nottinghamshire',
              'shrewsbury':'Shropshire', 
              'warwickshire':'Warwickshire'}

NCPlannedDF.replace({'Region': sapregdict}, 
                    inplace = True)

NCPlannedDF['SAPRegion'].fillna(NCPlannedDF['Region'], inplace = True)

#NCPlannedDF.drop('Region', axis = 'columns', inplace = True)

nullcg = NCPlannedDF[NCPlannedDF['ControlGroup'].isnull()]
givencgs = nullcg.groupby('Control Group').size().index
wantedcgs = DMA_NAME_CG_SR['ControlGroup'].unique()

[cg for cg in givencgs if cg not in wantedcgs]

cgstoreplace = {
 'Abbott Road / Berry Hill':'Abbott Rd/Berry Hill',
 'BML':'Birmingham Middle Level',
 'BML/LL':'Birmingham Middle Level',
 'Bignall Hill / Peckforton':'Bignall Hill/Peckforton',
 'Birmingham Mid Level':'Birmingham Middle Level',
 'Bowmere Heath':'Bomere Heath',
 'Bromsgrove ':'Bromsgrove',
 'Burrough/Barby':'Burrough/Barsby',
 'Coventry':'Coventry Middle Level',
 'Diamond Avenue':'Diamond Ave',
 'Little Eaton':'Derby Zone 3', #looked up manually
 'MHL/MML':'Mythe High Level',
 'ML & LL':'Birmingham Middle Level', #??
 'Mic':'Mitcheldean', #??
 'Micheldean':'Mitcheldean',
 'Mixon Hay / Moorlands':'Mixon Hay/Moorlands',
 'Mythe HL/ML':'Mythe High Level',
 'Mythe ML':'Mythe Medium Level',
 'Ogston':'Oxton',
 'RSA (Worcs)':'RSA Worcestershire',
 'Redhill':'Redhill EM', # i.e. redhill, nottinghamshire
 'Satnall':'Satnall/Stafford East',
 'Shelton/Old Park':'Shelton',
 'Stafford East/ Satnall':'Satnall/Stafford East',
 'Stourbridge and North Kidderminster':'Stourbridge',
 'Trimpley Rural':'Trimpley Rural Main',
 'Warley ':'Warley',
 'Warley Hollymoor':'Warley',
 'Whitacre':'Nuneaton',
 'Wolverhampton East ':'Wolverhampton East',
 'Wombourn':'Wombourne',
 'belle vue':'Belle Vue',
 'birmingham mid level':'Birmingham Middle Level',
 'multiple':'Multiple',
 'paplewick':'Ramsdale',
 'papplewick':'Ramsdale'}
 
NCPlannedDF['OrigCG'] = NCPlannedDF['Control Group']

NCPlannedDF.replace({'Control Group': cgstoreplace}, 
                    inplace = True)

NCPlannedDF['ControlGroup'].fillna(NCPlannedDF['Control Group'],
                                   inplace = True)

NCPlannedDF.drop(['Control Group', 'Region'], axis = 'columns', inplace = True)

NCPlannedDF['Description'] = NCPlannedDF['Description'].apply(
    lambda s: pd.np.nan if type(s) != str else s)









  

