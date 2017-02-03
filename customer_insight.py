# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 09:48:13 2016

@author: 142796
"""

import matplotlib
#matplotlib.use("Qt4Agg")

import matplotlib.pyplot as plt

import pandas as pd
import os
import seaborn
#import numpy as np

from stwpy.complaints_reader import ComplaintsDFWithRegions as ComplaintsDFwithregions

ComplaintsDFwithregions['Cluster'] = ComplaintsDFwithregions['ConsecutiveDaysDMAClusterSize'] >= 3


# In[]

#regionsdemopiv = pd.pivot_table(
#    ComplaintsDFwithregions[ComplaintsDFwithregions['Translated']=='Appearance'][['Acorn5', 'SAP Area - Description']], 
#    index = 'Acorn5', 
#    columns = 'SAP Area - Description', 
#    aggfunc = len)
#regionsdemopiv.plot.bar(stacked = True)

#%% Cool KDE plot (probably pointless but shows weekly cycle and exceptionals)

FilteredDF = ComplaintsDFwithregions[
    (ComplaintsDFwithregions['Year'] == 2016)
    #& (ComplaintsDFwithregions['SAP Area - Description']=='Central')
    ]

ax = seaborn.distplot(
                      FilteredDF[FilteredDF['Cluster']==True]['OrdinalDateTime'].dropna(), 
                      bins = 365,
                      kde_kws = {'bw':3, 'gridsize':3650},
                      color = 'green',
                      )

ax1 = seaborn.distplot(
                      FilteredDF[FilteredDF['Cluster']==False]['OrdinalDateTime'].dropna(), 
                      bins = 365,
                      kde_kws = {'bw':0.5, 'gridsize':3650},
                      color = 'blue',
                      ax = ax,
                      )

ax.set_xlim(FilteredDF['OrdinalDateTime'].min(), FilteredDF['OrdinalDateTime'].max())
ax.set_ylim(0,0.2)

plt.show()

#ax.set_xticklabels(labels = ['a','b','c'])

#TASK: break down into the DMA level to get distribution of complaints 
#vs households in each DMA: hence get a "likeliness to complain" stat.

#%% All complaints by time of day

fig3 = ComplaintsDFwithregions[[
    'DecimalHour']].hist(bins = 48, 
                         stacked = True, 
                         color = 'blue')
    
fig3[0,0].set_title('Complaints by time of day')

plt.show()

#%% Weekday complaints by region: plot

fig1 = ComplaintsDFwithregions[
    ComplaintsDFwithregions['DayOfWeek'] <5][[
    'DecimalHour']].hist(by = ComplaintsDFwithregions[
                            'SAP Area - Description'], 
                         bins = 24, 
                         layout = (4,2),
                         stacked = True,
                         figsize = (10,10),
                         color = 'green'
                         )
    
for i in range(4):
    for j in range(2):
        fig1[i,j].set_xlim(0,24)
        fig1[i,j].set_ylabel('Total complaints')
        fig1[i,j].set_xlabel('Hour of day')
        fig1[i,j].set_xticks([0,3,6,9,12,15,18,21,24])
        fig1[i,j].set_xticklabels([0,3,6,9,12,15,18,21,24])

plt.suptitle('Weekday complaints by region', fontsize = 14)
seaborn.set_style(style = 'darkgrid')
#seaborn.set_palette("Reds")
plt.show()

#%% Weekend complaints by region: plot

fig1 = ComplaintsDFwithregions[
    ComplaintsDFwithregions['DayOfWeek'] >=0][[
    'DecimalHour']].hist(by = ComplaintsDFwithregions[
                            'SAP Area - Description'], 
                         bins = 24, 
                         layout = (4,2),
                         stacked = True,
                         figsize = (10,10),
                         color = 'green'
                         )
    
for i in range(4):
    for j in range(2):
        fig1[i,j].set_xlim(0,24)
        fig1[i,j].set_ylabel('Total complaints')
        fig1[i,j].set_xlabel('Hour of day')
        fig1[i,j].set_xticks([0,3,6,9,12,15,18,21,24])
        fig1[i,j].set_xticklabels([0,3,6,9,12,15,18,21,24])

plt.suptitle('All complaints by region', fontsize = 14)
seaborn.set_style(style = 'darkgrid')
#seaborn.set_palette("Reds")
plt.show()

#%% Clustered vs non-clustered

def cwcat(row):
    if row['Cluster']:
        a = 'clustered'
    else:
        a = 'non-clustered'
    if row['DayOfWeek'] < 5:
        b = 'Weekday, '
    else:
        b = 'Weekend, '
    return b + a

ComplaintsDFwithregions['Cluster/Weekend'] = ComplaintsDFwithregions.apply(cwcat, axis = 1)

fig = ComplaintsDFwithregions[
    'DecimalHour'].hist(by = ComplaintsDFwithregions['Cluster/Weekend'],
                        bins = 48, 
                        stacked = True, 
                        color = 'green',
                        #sharey = True
                        )
  
    
plt.suptitle('Complaints by time of day', fontsize= 14)

for i in [0,1]:
    for j in [0,1]:
        fig[i,j].set_xlim(0,24)
        #fig[i,j].set_ylim(0,4000)
        #fig[i].set_title(['Non-clustered', 'Clustered'][i])
        fig[i,j].set_ylabel('Total complaints')
        fig[i,j].set_xlabel('Hour of day')
        fig[i,j].set_xticks([0,3,6,9,12,15,18,21,24])
        fig[i,j].set_xticklabels([0,3,6,9,12,15,18,21,24])

plt.show()

#%% Time of day breakdown by acorn group (normalised)

from stwpy.complaints_reader import Acorn5Info

piv = pd.pivot_table(ComplaintsDFwithregions[['Acorn5', 'Hour']], 
                     index = 'Acorn5', 
                     columns = 'Hour', 
                     aggfunc = len)

piv = piv.join(Acorn5Info)

pivnormed = pd.DataFrame()

for i in range(24):
    pivnormed[i] = 1000*piv[i]/piv['TOTALHOUSEHOLDS']

fig, ax = plt.subplots(nrows = 5, 
                       ncols = 1, 
                       #sharex = True,
                       figsize = (8,10))

acornnames = {'C1':'Affluent Achievers',
              'C2':'Rising Prosperity',
              'C3':'Comfortable Communities',
              'C4':'Financially Stretched',
              'C5':'Urban Adversity'}

for i in range(5):
    acorn = Acorn5Info.index[1:][i]
    data = pivnormed.ix[acorn]
    data.plot(kind = 'bar', 
              ax = ax[i],
              width = 1)
    ax[i].set_ylim(0,5)
    ax[i].set_title('Category ' + acorn[-1] + ": " + acornnames[acorn], fontsize = 14)

ax[-1].set_xlabel('Hour of day', fontsize = 12)
ax[2].set_ylabel('Complaints per 1000 households', fontsize = 14)

fig.subplots_adjust(hspace = .7)
    
#plt.suptitle('Complaint time by demographic group', fontsize = 14)
    
plt.show()
    
#%%
from stwpy.complaints_reader import Acorn17Info


piv = pd.pivot_table(ComplaintsDFwithregions[['Acorn17', 'Hour']],
                     index = 'Acorn17',
                     columns = 'Hour',
                     aggfunc = len)

piv = piv.join(Acorn17Info)

pivnormed = pd.DataFrame()

for i in range(24):
    pivnormed[i] = 1000*piv[i]/piv['TOTALHOUSEHOLDS']

fig, ax = plt.subplots(nrows = 18,
                       ncols = 1,
                       sharex = True,
                       figsize = (8,36))

for i in range(18):
    acorn = Acorn17Info.index[:][i]
    data = pivnormed.ix[acorn]
    data.plot(kind = 'bar',
              ax = ax[i],
              width = 1)
    #ax[i].set_ylim(0,5)
    ax[i].set_title('Acorn Category ' + acorn)

ax[-1].set_xlabel('Hour of day')
ax[2].set_ylabel('Complaints per 1000 households')

fig.subplots_adjust(hspace = 1)
    
plt.suptitle('Complaint time by demographic group', fontsize = 14)
    
plt.show()
# TODO: normalise demographic time breakdowns by households. then continue!

#%%

from stwpy.dmautils import DMAsNetbase
from stwpy.complaints_reader import PostcodeAcorn

#%%

DMAPops = PostcodeAcorn.groupby(by = 'ID_DMA_COD')['TOTALHOUSEHOLDS'].sum()

DMAsNetbase = DMAsNetbase.set_index('Reference')
#%%
DMAsJoined = DMAsNetbase.join(DMAPops)

SAPRegionHHs = DMAsJoined.groupby(by = 'SAP Area - Description')['TOTALHOUSEHOLDS'].sum()

SapAreas = pd.DataFrame(ComplaintsDFwithregions.groupby(by = 'SAP Area - Description').size())

SapAreas.columns = ['TotalComplaints']
SapAreas['TotalHouseholds'] = SAPRegionHHs
SapAreas['ComplaintsPer1000Households'] = 1000*SapAreas['TotalComplaints']/SapAreas['TotalHouseholds']

fig  = SapAreas['ComplaintsPer1000Households'].sort_values(ascending = False).plot(
           kind = 'bar',
           color = 'green',
           figsize = (10,6)
           )

fig.set_ylabel('Complaints per 1000 households', fontsize=14)
fig.set_xlabel('SAP Area', fontsize = 14)
#fig.set_title('Complaints per 1000 households across the eight SAP areas', fontsize=14)

plt.show()
#%%
DMAsJoined = DMAsNetbase.join(DMAPops)

SAPRegionHHs = DMAsJoined.groupby(by = 'SAP Area - Description')['TOTALHOUSEHOLDS'].sum()

SapAreas = pd.DataFrame(ComplaintsDFwithregions.groupby(by = 'SAP Area - Description').size())

SapAreas.columns = ['TotalComplaints']
SapAreas['TotalHouseholds'] = SAPRegionHHs
SapAreas['ComplaintsPer1000Households'] = 1000*SapAreas['TotalComplaints']/SapAreas['TotalHouseholds']

fig  = SapAreas.sort_values(by = 'ComplaintsPer1000Households', ascending = False)['TotalComplaints'].plot(
           kind = 'bar',
           color = 'green',
           figsize = (10,6)
           )

fig.set_ylabel('Total complaints', fontsize=14)
fig.set_xlabel('SAP Area', fontsize = 14)
#fig.set_title('Complaints per 1000 households across the eight SAP areas', fontsize=14)

plt.show()

#%%

fig, ax = plt.subplots()

acorn17names = {'A':'Lavish Lifestyles',
                'B':'Executive Wealth',
                'C':'Mature Money',
                'D':'City Sophisticates',
                'E':'Career Climbers',
                'F':'Countryside Communities',
                'G':'Successful Suburbs',
                'H':'Steady Neighbourhoods',
                'I':'Comfortable Seniors',
                'J':'Starting Out',
                'K':'Student Life',
                'L':'Modest Means',
                'M':'Striving Families',
                'N':'Poorer Pensioners',
                'O':'Young Hardship',
                'P':'Struggling Estates',
                'Q':'Difficult Circumstances'}

Acorn17Info1 = Acorn17Info.drop('Business')
Acorn17Info1['Name'] = Acorn17Info1.index
Acorn17Info1['Name'] = Acorn17Info1['Name'].map(
    lambda a: a + ": " + acorn17names[a])

colors = [0,0,0,3,3,1,1,1,1,1,4,4,4,4,5,5,5]
colors = [seaborn.color_palette(palette='deep')[i] for i in colors]

Acorn17Info1['TotalComplaints'].plot(
        kind = 'bar',
        ax=ax,
        figsize = (6,6),
        color = colors)

ax.set_ylabel('Total Complaints', fontsize = 14)
ax.set_xlabel('Acorn Group', fontsize = 14)
ax.set_title('Total complaints by Acorn group', fontsize = 14)

plt.show()

#%% Table for Marianne

def cwcat(row):
    if row['Cluster']:
        a = 'clustered'
    else:
        a = 'non-clustered'
    if row['DayOfWeek'] < 5:
        b = 'Weekday, '
    else:
        b = 'Weekend, '
    return b + a

ComplaintsDFwithregions['Cluster/Weekend'] = ComplaintsDFwithregions.apply(cwcat, axis = 1)

ComplaintsDFwithregions.groupby(
    by = ['Year',
          'Cluster/Weekend',
          'Hour']).size().unstack().fillna(0).to_csv(
              'ComplaintsByTimeOfDayPivot.csv'
              )
#%%

# In[]
