# -*- coding: utf-8 -*-
"""
ABCanalysis.py
This script contains functions to join event data based on time and space
constraints, and to combine these into A --> B --> C style analysis (e.g. 
works --> anomalies --> complaints)

@author: 142796
"""

#%% Import modules

import pandas as pd
get_events_from_pickle = True

#%% Import data

def firsttwochars(s):
    try:
        return s[:2]
    except:
        return pd.np.nan

from stwpy.complaints_reader import ComplaintsDFWithRegions as CDF

#ComplaintsFilteredBordesley = CDF[
#       (CDF['Translated']=='Appearance')
#     & (CDF['DateTime'] >= pd.to_datetime('2016-01-01'))
#     & (CDF['DateTime'] <= pd.to_datetime('2016-04-01'))
#     & (CDF['WQZ Name']=='Bordesley')]

ComplaintsFiltered = CDF[
       (CDF['Translated']=='Appearance')
     & (CDF['DateTime'] >= pd.to_datetime('2015-05-01'))
     & (CDF['DateTime'] < pd.to_datetime('2016-07-01'))
     & (CDF['DMA'].apply(firsttwochars)=='04')]

#ComplaintsFiltered = CDF[
#      (CDF['Translated']=='Appearance')]

if get_events_from_pickle:
    events = pd.read_pickle('PickledData/FullEventsList.pkl')
else:   
    from allinterventions import filtered as events        

#%% applyfilter



events = events[
    (events['DMA'].apply(firsttwochars)=='04') 
    & (events['Start']>=pd.to_datetime('2015-05-01'))
    & (events['Start']<pd.to_datetime('2016-07-01'))]


#filteredevents['DMAfirsttwochars'] = filtered


#filteredevents = filteredevents[filteredevents['WQZ']=='Bordesley']    
    
#filteredevents['Op'] = filteredevents.index
#filteredevents['Op'] = filteredevents['Op'].apply(
#    lambda tup: str(int(tup[0])) + "/" + str(int(tup[1]))
#    )
 #%%
 
import os

anomaliesdir = "../Anomalies csv files/"

#infiles = ["AO3  anomalies_Jan_2015_Jun_2016.csv",
#           "AO4  anomalies_Jan_2015_Jun_2016.csv"]

infiles = os.listdir(anomaliesdir)

#infile1 = "AO3  anomalies_Jan_2015_Jun_2016.csv"
#infile2 = "AO4  anomalies_Jan_2015_Jun_2016.csv"
#
#anomalies1 = pd.read_csv(anomaliesdir+infile1, 
#                        parse_dates = ['ReadingHour'], 
#                        dayfirst = True)
#
#anomalies2 = pd.read_csv(anomaliesdir+infile2, 
#                        parse_dates = ['ReadingHour'], 
#                        dayfirst = True)

anomalies = pd.DataFrame()

for infile in infiles:
    anomalies = anomalies.append(
                        pd.read_csv(
                            anomaliesdir+infile, 
                            parse_dates = ['ReadingHour'], 
                            dayfirst = True),
                            ignore_index=True)

#anomalies = anomalies1.append(anomalies2, ignore_index=True)

anomalies = anomalies.drop_duplicates(subset = ['ReadingHour', 'SensorFileName'])

#%%

#logdictdir = "S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/LoggerDetails/"
#logdictinfile = "FullLoggerDictFuzzy.csv"
#
#loggerdict = pd.read_csv(logdictdir+logdictinfile, 
#                         encoding='latin-1')

from dma_mapper_copy import d

anomalies['DMA'] = anomalies['SensorID'].map(d)
anomalies['DMA'] = anomalies['DMA'].apply(lambda s: s.zfill(5))

from stwpy.dmautils import DMA_NAME_WQZ_CG_SR

anomalies = anomalies.join(DMA_NAME_WQZ_CG_SR, 
                           on = 'DMA')

def get_loggers_for_dma(dma):
    return [a for a in d.keys() if d[a].zfill(5)==dma]



#bordesley_loggers = pd.read_csv('bordesley_loggers.csv')
#bordesley_dict = bordesley_loggers.set_index('HalmaLoggerIndex').to_dict()
#
#anomalies['DMA'] = [str(bordesley_dict['DMA'][x]).zfill(5) \
#                     for x in anomalies['SensorID']]
#
#
## Quickly rename columns to match below...
#
#anomalies['EndDate'] = anomalies['ReadingHour'].dt.date
#anomalies['Month'] = anomalies['ReadingHour'].dt.month
#anomalies['WQZ'] = 'Bordesley'
#anomalies['Index'] = anomalies['Unnamed: 0']

#%% Ancillary functions

# DMA neighbour series (indexed like a dictionary)
from stwpy.dmautils import DMAneighbours

def are_neighbours(dma1, dma2):
    if pd.isnull(dma1) or pd.isnull(dma2):
        return pd.np.nan
    else:
        try:
            return dma2 in DMAneighbours[dma1]
        except:
            return False

def get_potential_causes(
        causesdf,   
        effectsdf, 
        causes_index,
        effects_index,
        cause_dt,
        effect_dt,
        cause_suffix='Cause',
        effect_suffix='Effect',
        neighbouring_dmas = True,
        dma_adjacency_fn = are_neighbours,
        time_window_hours = None,
        ):
    """
    Gets the potential "causes" of events in effectsdf. In this function, 
    a cause is anything in causesdf which occurs on the same day or the day 
    before the effect, and in the same DMA or in a neighbouring one.
    
    Both tables should have 'DMA' (string, length 5) fields. You must specify 
    an index and datetime fields for each too.
    """
    causesdf = causesdf.copy()
    effectsdf = effectsdf.copy()
    quick_return = False
    oneday = pd.Timedelta('1 day')
    if 'Date' not in effectsdf.columns:
        effectsdf['Date'] = effectsdf[effect_dt].dt.date
    if 'Date' not in causesdf.columns:
        causesdf['Date'] = causesdf[cause_dt].dt.date    
    causesdf['Date+1'] = causesdf['Date'].apply(lambda d: d + oneday)
    causesdf.columns = causesdf.columns + cause_suffix
    effectsdf.columns = effectsdf.columns + effect_suffix                      
    SameDayMerge = pd.merge(
        causesdf[['Date' + cause_suffix, 
                  cause_dt + cause_suffix, 
                  'Date+1' + cause_suffix, 
                  'DMA' + cause_suffix, 
                  causes_index + cause_suffix]], 
        effectsdf[['Date' + effect_suffix, 
                   effect_dt + effect_suffix, 
                   'DMA' + effect_suffix,
                   effects_index + effect_suffix]], 
        how = 'inner', # was 'right' 
        left_on = 'Date' + cause_suffix, 
        right_on = 'Date' + effect_suffix, 
        indicator = True)
    SameDayMerge['SameOrNextDay'] = 'SameDay'
    NextDayMerge = pd.merge(
        causesdf[['Date' + cause_suffix,
                  cause_dt + cause_suffix,
                  'Date+1' + cause_suffix,
                  'DMA' + cause_suffix,
                  causes_index + cause_suffix]], 
        effectsdf[['Date' + effect_suffix,
                   effect_dt + effect_suffix,
                   'DMA' + effect_suffix
                   , effects_index + effect_suffix]], 
        how = 'inner', 
        left_on = 'Date+1' + cause_suffix, 
        right_on = 'Date' + effect_suffix, 
        indicator = True)
    NextDayMerge['SameOrNextDay'] = 'NextDay'
    SameOrNextDayMerge = SameDayMerge.append(NextDayMerge)
    del SameDayMerge, NextDayMerge
    #Filter returning only those rows of the joined table where the DMAs of 
    #anomaly and complaint are adjacent
#    if neighbouring_dmas:
#        DMAneighbourfilter = SameOrNextDayMerge[[
#            'DMA' + cause_suffix, 'DMA'+ effect_suffix
#            ]].apply(
#                lambda x: dma_adjacency_fn(x[0], x[1]), axis = 1
#                )
#    else:
#        DMAneighbourfilter = SameOrNextDayMerge['DMA' + cause_suffix] \
#                              ==SameOrNextDayMerge['DMA' + effect_suffix]
    SameOrNextDayMerge['NeighbouringDMA_' + cause_suffix + effect_suffix] = \
        SameOrNextDayMerge[[
            'DMA' + cause_suffix, 'DMA'+ effect_suffix
            ]].apply(
                lambda x: dma_adjacency_fn(x[0], x[1]), axis = 1
                )
    SameOrNextDayMerge['SameDMA_' + cause_suffix + effect_suffix] = \
        SameOrNextDayMerge['DMA' + cause_suffix] \
         == SameOrNextDayMerge['DMA'+ effect_suffix]
    dmafilter = (SameOrNextDayMerge[
                    'SameDMA_' + cause_suffix + effect_suffix]) | \
                (SameOrNextDayMerge[
                    'NeighbouringDMA_' + cause_suffix + effect_suffix])
    print(SameOrNextDayMerge.columns)
    if time_window_hours != None:
        time_window = pd.Timedelta(hours = time_window_hours)
        zero_td = pd.Timedelta(hours = 0)
        time_filter = (SameOrNextDayMerge[effect_dt + effect_suffix] \
                      - SameOrNextDayMerge[cause_dt + cause_suffix] \
                      <= time_window) & \
                      (SameOrNextDayMerge[effect_dt + effect_suffix] \
                      - SameOrNextDayMerge[cause_dt + cause_suffix] \
                      >= zero_td)
        SameOrNextDayMerge['_time_filter'] = time_filter
        SameOrNextDayMerge['_dmafilter'] = dmafilter
        SameOrNextDayMerge = SameOrNextDayMerge[time_filter & dmafilter]
    if quick_return:
        return SameOrNextDayMerge
    #Join again with the original complaints table    
    else:
        SameOrNextDayMerge['Count'] = 1
        EffectsLJCauses = pd.merge(
            effectsdf[[effects_index + effect_suffix]],
            SameOrNextDayMerge[[
                causes_index + cause_suffix, 
                effects_index + effect_suffix,
                'Count']],
            how = 'left',
            on = effects_index + effect_suffix
            )
        EffectsLJCauses['Count'] = EffectsLJCauses['Count'].fillna(0)
        EffectCauseCounts = EffectsLJCauses.groupby(
            by = effects_index + effect_suffix)['Count'].sum()
        EffectCauseCounts.name = 'NumberOf' + cause_suffix
        EffectsUniqueLJCounts = effectsdf.join(EffectCauseCounts, 
                                               effects_index + effect_suffix)
        EffectsUniqueLJCounts['AtLeastOne' + cause_suffix] \
            = EffectsUniqueLJCounts['NumberOf' + cause_suffix] > 0
        CausesLJEffects = pd.merge(
            causesdf[[causes_index + cause_suffix]],
            SameOrNextDayMerge[[
                causes_index + cause_suffix, 
                effects_index + effect_suffix,
                'Count']],
            how = 'left',
            on = causes_index + cause_suffix
            )
        CausesLJEffects['Count'] = CausesLJEffects['Count'].fillna(0)
        CauseEffectCounts = CausesLJEffects.groupby(
            by = causes_index + cause_suffix)['Count'].sum()
        CauseEffectCounts.name = 'NumberOf' + effect_suffix
        CausesUniqueLJCounts = causesdf.join(
            CauseEffectCounts, causes_index + cause_suffix)
        CausesUniqueLJCounts['AtLeastOne' + effect_suffix] \
            = CausesUniqueLJCounts['NumberOf' + effect_suffix] > 0
        SlimInner = SameOrNextDayMerge[[causes_index + cause_suffix, 
                                        effects_index + effect_suffix]]
#        Outer = pd.merge(pd.merge(causesdf, 
#                                  SlimInner,
#                                  how = 'left',
#                                  on = causes_index + cause_suffix),
#                         effectsdf,
#                         how = 'outer',
#                         on = effects_index + effect_suffix
#                         )
        Outer = pd.merge(pd.merge(CausesUniqueLJCounts, 
                                  SlimInner,
                                  how = 'left',
                                  on = causes_index + cause_suffix),
                         EffectsUniqueLJCounts,
                         how = 'outer',
                         on = effects_index + effect_suffix
                         )        
        return CausesUniqueLJCounts, EffectsUniqueLJCounts, Outer

#%% Run the function        

#Choose the data for A and C

DF_A = events
DF_A['Index'] = DF_A.index
DF_A_suffix = 'Cause'
DF_A_datetime = 'Start'
DF_A_index = 'Index'

DF_C = ComplaintsFiltered
DF_C['Index'] = DF_C.index
DF_C_suffix = 'Complaint'
DF_C_datetime = 'DateTime'
DF_C_index = 'Index'

#%%

#Choose the data for B (optional)

DF_B = anomalies
DF_B['Index'] = DF_B.index
DF_B_suffix = 'Anomaly'
DF_B_datetime = 'ReadingHour'
DF_B_index = 'Index'

#%%

AwithC, CwithA, ACouter = get_potential_causes(
                            DF_A, 
                            DF_C,
                            cause_suffix = DF_A_suffix,
                            effect_suffix = DF_C_suffix,
                            causes_index = DF_A_index,
                            effects_index = DF_C_index,
                            cause_dt = DF_A_datetime,
                            effect_dt = DF_C_datetime,
                            time_window_hours=24)

#AwithC.to_pickle('PickledData/FullEventsWithComplaints.pkl') 
#CwithA.to_pickle('PickledData/FullComplaintsWithEvents.pkl') 
#ACouter.to_pickle('PickledData/ComplaintsEventsOJ.pkl')



        
AwithB, BwithA, ABouter = get_potential_causes(
                            DF_A, 
                            DF_B,
                            cause_suffix = DF_A_suffix,
                            effect_suffix = DF_B_suffix,
                            causes_index = DF_A_index,
                            effects_index = DF_B_index,
                            cause_dt = DF_A_datetime,
                            effect_dt = DF_B_datetime,
                            time_window_hours=24)


BwithC, CwithB, BCouter = get_potential_causes(
                            DF_B, 
                            DF_C,
                            cause_suffix = DF_B_suffix,
                            effect_suffix = DF_C_suffix,
                            causes_index = DF_B_index,
                            effects_index = DF_C_index,
                            cause_dt = DF_B_datetime,
                            effect_dt = DF_C_datetime,
                            time_window_hours=24)

AwithBandC = pd.merge(AwithB,
                      AwithC[[DF_A_index + DF_A_suffix, 
                              'AtLeastOne' + DF_C_suffix, 
                              'NumberOf' + DF_C_suffix]],
                      how = 'left',
                      on = DF_A_index + DF_A_suffix)

#m2 = get_potential_causes(anomalies, 
#                          CDF,
#                          cause_suffix = 'Anomaly',
#                          effect_suffix = 'Complaint',
#                          causes_index = 'Index',
#                          effects_index = 'ComplaintRef',
#                          cause_dt = 'ReadingHour',
#                          effect_dt = 'DateTime',
#                          time_window_hours=24)

#causes, effects, outer = get_potential_causes(
#                             causesdf = filteredevents, 
#                             effectsdf = CDF, 
#                             cause_suffix = 'Cause', 
#                             effect_suffix = 'Effect', 
#                             causes_index = 'Op', 
#                             effects_index = 'ComplaintRef', 
#                             cause_dt = 'Start', 
#                             effect_dt = 'DateTime', 
#                             time_window_hours=24
#                             )
#
## TODO: Wrap into one function the analysis for three categories of event;
## i.e. a --> b --> c (such as works --> anomalies --> complaints)
#
## TODO: Include the groupby statements
#
#causes.groupby(by = ['AtLeastOneEffect']) # add extra fields to group by here
#effects.groupby(by = ['AtLeastOneCause']) # add extra fields to group by here

# TODO: Add in a line to validate the time windows and the areas...

# We can run the function for different categories of event with different 
# methodologies

CwithB.groupby(by = 'AtLeastOneAnomaly').size()

#%% #filter for actual "works":

works = ['Valve Op', 
         'Mains Repair',
         'SAP Planned work', 
         'SAP Unplanned work']

bursts = ['Communication Pipe Burst',
          'Fitting Burst',
          'Main Burst',
          'Supply Pipe Burst']
    
AwithBworks = AwithB[(AwithB['InterventionTypeCause'].isin(works))]

AwithBandCworks = AwithBandC[(AwithBandC['InterventionTypeCause'].isin(works))]

AnomaliesFromWorks = BwithC[
    BwithC['IndexAnomaly'].isin(
        ABouter[ABouter['InterventionTypeCause'].isin(works)][
            'IndexAnomaly'])
                ]
    
AnomaliesFromBursts = BwithA[
    BwithA['IndexAnomaly'].isin(
        ABouter[ABouter['InterventionTypeCause'].isin(bursts)][
            'IndexAnomaly'])
                ]    
    
    
AnomaliesNotFromWorks = BwithC[
    ~(BwithC['IndexAnomaly'].isin(
        ABouter[ABouter['InterventionTypeCause'].isin(works)][
            'IndexAnomaly']))
                ]    

BwithC['FromWorks'] = (
    BwithC['IndexAnomaly'].isin(
        ABouter[ABouter['InterventionTypeCause'].isin(works)][
            'IndexAnomaly'])
                )

ComplaintsFromWorkRelatedAnomalies = CwithB[
    CwithB['IndexComplaint'].isin(
        BCouter[BCouter['IndexAnomaly'].isin(
            AnomaliesFromWorks['IndexAnomaly'])][
                'IndexComplaint'])
                    ]

ComplaintsFromNonWorkRelatedAnomalies = CwithB[
    ~(CwithB['IndexComplaint'].isin(
        BCouter[BCouter['IndexAnomaly'].isin(
            AnomaliesFromWorks['IndexAnomaly'])][
                'IndexComplaint']))
                    ]

CwithB['FromWorkRelatedAnomaly'] = CwithB['IndexComplaint'].isin(
        BCouter[BCouter['IndexAnomaly'].isin(
            AnomaliesFromWorks['IndexAnomaly'])][
                'IndexComplaint'])

#AnomaliesFromAnything = BwithC[
#    BwithC['IndexAnomaly'].isin(
#        ABouter[ABouter['InterventionTypeCause'].notnull()][
#            'IndexAnomaly'])
#                ]

AnomaliesNotFromWorks.groupby(by = 'AtLeastOneComplaint').size()
AnomaliesFromWorks.groupby(by = 'AtLeastOneComplaint').size()

#%% Anomalies leading to complaints
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize = (6,6))

#BwithC[BwithC['AtLeastOneComplaint']==True][['NumberOfComplaint']].boxplot(
#        ax=ax)

#BwithC[]

BwithC[BwithC['AtLeastOneComplaint']==True]['NumberOfComplaint'].hist(
        ax=ax, bins = 200)

#ax.set_ylim((0,50))

ax.set_xlim((0, 30))

#ax.set_xticklabels([])

ax.set_ylabel('Number of anomalies', fontsize = 14)
ax.set_xlabel('Number of complaints', fontsize = 14)

ax.set_title('Anomalies which are followed by complaints', fontsize = 14)

#%% Work types leading to complaints: percentage

piv = AwithB[['InterventionTypeCause', 
                            'AtLeastOneAnomaly']].pivot_table(
    index = ['InterventionTypeCause'],
    columns = ['AtLeastOneAnomaly'], 
    aggfunc = len)

piv['%'] = 100*piv[True]/(piv[True]+piv[False])

piv = piv['%'].transpose()

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv = piv.ix[workcols]

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
piv.plot(kind = 'bar', ax=ax)
ax.set_ylabel('% followed by anomalies', fontsize=14)
ax.set_xlabel('Work type', fontsize=14)

plt.suptitle('Works and bursts leading to anomalies', fontsize=14)

plt.show()





#%% Groupbys for numbers

# Works followed by anomalies
AwithBworks.groupby(by = 'AtLeastOneAnomaly').size()
596/(596+2767) # 18%

# Anomalies accounted for by works
AnomaliesFromWorks.shape[0] / BwithA.shape[0] # 14%

# Anomalies accounted for by bursts
AnomaliesFromBursts.shape[0] / BwithA.shape[0] # 14%                        
                        
# Anomalies leading to complaints
BwithC.groupby(by = 'AtLeastOneComplaint').size()
359/(359+2830)

# Complaints attributable to anomalies (including clusters)
CwithB.groupby(by = 'AtLeastOneAnomaly').size()
649/(649+2543)

CwithB[CwithB['ConsecutiveDaysDMAClusterSizeComplaint']>=3].groupby(by = ['Incident RelatedComplaint','AtLeastOneAnomaly']).size()
459/(459+1030)

CwithB[CwithB['ConsecutiveDaysDMAClusterSizeComplaint']>=10].groupby(by = ['Incident RelatedComplaint','AtLeastOneAnomaly']).size()
271/(271+196)

#Number of complaints in large clusters
CwithB[CwithB['ConsecutiveDaysDMAClusterSizeComplaint']>=10].shape


#%% A basic Poisson regression for the work types

import numpy as np
import statsmodels.formula.api as smf
#import statsmodels.api as sm
from scipy.stats.stats import pearsonr

#mod = smf.poisson('NumberOfComplaint ~ C(InterventionTypeCause + SAPRegionCause)', data=AwithC).fit()
mod = smf.poisson('NumberOfComplaint ~ C(InterventionTypeCause)', data=AwithC).fit()


#mod = smf.poisson('NumberOfEffect ~ 1', data=causes).fit()

print(mod.mle_retvals['converged'])

mod.summary()

#%%

from allinterventions import gpd

gpd['Op'] = gpd.index
gpd['Op'] = gpd['Op'].apply(
    lambda tup: str(int(tup[0])) + "/" + str(int(tup[1]))
    )

causes, effects, outer = get_potential_causes(
                             causesdf = gpd, 
                             effectsdf = CDF, 
                             cause_suffix = 'Cause', 
                             effect_suffix = 'Effect', 
                             causes_index = 'Op', 
                             effects_index = 'ComplaintRef', 
                             cause_dt = 'Start', 
                             effect_dt = 'DateTime', 
                             time_window_hours=24
                             )

mod = smf.poisson('NumberOfEffect ~ C(TypeCause)', data=causes).fit()
#mod = smf.poisson('NumberOfEffect ~ 1', data=causes).fit()

print(mod.mle_retvals['converged'])

# TODO: Filter NC incidents to remove "WQ events".

#%% BASELINE

#%% BASELINE

# Want to find out, for a random sample of events, what proportion would lead
# to complaints?

# Use the same DMAs as the given events.

def make_random_events(events, size):
    m = events.reset_index().shape[0]
    n1 = pd.np.random.randint(0, high = m, size = size)
    print(n1[:10])
    n2 = pd.np.random.randint(0, high = m, size = size)
    print(n2[:10])
    s1 = events.reset_index()['StartCause'].ix[n1].reset_index()
    s2 = events.reset_index()['DMACause'].ix[n2].reset_index()
    randomeventsdf = pd.DataFrame()
    randomeventsdf['Start'] = s1['StartCause']
    randomeventsdf['DMA'] = s2['DMACause']
    randomeventsdf['Index'] = randomeventsdf.index
    return randomeventsdf

RandomDF = make_random_events(AwithC, 3189)

randomAwithC, randomCwithA, randomACouter = get_potential_causes(
                             causesdf = RandomDF, 
                             effectsdf = ComplaintsFiltered, 
                             cause_suffix = 'Cause', 
                             effect_suffix = 'Effect', 
                             causes_index = 'Index', 
                             effects_index = 'ComplaintRef', 
                             cause_dt = 'Start', 
                             effect_dt = 'DateTime', 
                             time_window_hours=24
                             )

randomAwithC.groupby(by = 'NumberOfEffect').size()











