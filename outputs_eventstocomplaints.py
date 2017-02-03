# -*- coding: utf-8 -*-
"""
This script imports the data of complaints linked to events from pickle files, 
and then makes lots of plots which are used in the final report.

@author: 142796
"""

import pandas as pd
import seaborn
#%%
# Read in the data

EventsWithComplaints = pd.read_pickle('PickledData/FullEventsWithComplaints.pkl') 
ComplaintsWithEvents = pd.read_pickle('PickledData/FullComplaintsWithEvents.pkl') 
FullComplaintsEventsOJ = pd.read_pickle('PickledData/ComplaintsEventsOJ.pkl')

#%% Filtering down the events data

EventsWithComplaints = EventsWithComplaints[
    (EventsWithComplaints['StartCause'] < pd.to_datetime('2016-07-01'))
    & (EventsWithComplaints['InterventionTypeCause'] != 'Interruption')
    & (EventsWithComplaints['InterventionTypeCause'] != 'NC Incident')
    & (EventsWithComplaints['InterventionTypeCause'] != 'NC Planned work')
    ]

#%% Work types leading to complaints: percentage
EventsWithComplaints['Year'] = EventsWithComplaints['StartCause'].dt.year
EventsWithComplaints = EventsWithComplaints[EventsWithComplaints['Year']>=2014]

#%%

piv = EventsWithComplaints[['InterventionTypeCause', 
                            'Year', 
                            'AtLeastOneComplaint']].pivot_table(
    index = [ 'Year', 'InterventionTypeCause'],
    columns = ['AtLeastOneComplaint'], 
    aggfunc = len)

piv['%'] = 100*piv[True]/(piv[True]+piv[False])

piv = piv['%'].unstack().transpose()

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
piv.plot(kind = 'bar', ax=ax)
ax.set_ylabel('% followed by complaints', fontsize=14)
ax.set_xlabel('Work type', fontsize=14)

l = ax.get_legend()
l.get_texts()[2].set_text('2016 (to June)')
plt.suptitle('Works and bursts leading to complaints', fontsize=14)

plt.show()

#%% Work types leading to complaints: raw

piv = EventsWithComplaints[['InterventionTypeCause', 
                            'Year', 
                            'AtLeastOneComplaint']].pivot_table(
    index = [ 'Year', 'InterventionTypeCause'],
    columns = ['AtLeastOneComplaint'], 
    aggfunc = len)

piv = piv[True].unstack().transpose()

fig, ax = plt.subplots()
piv.plot(kind = 'bar', ax=ax)
ax.set_ylabel('Total followed by complaints', fontsize=14)
ax.set_xlabel('Work type', fontsize=14)
l = ax.get_legend()
l.get_texts()[2].set_text('2016 (to June)')
plt.suptitle('Works and bursts leading to complaints', fontsize=14)

plt.show()

#%% Complaints with potential attributions to work types: percentages

def nonempty(l):
    if len(l)>0:
        return 1
    else:
        return 0
    
piv = FullComplaintsEventsOJ[[
        'IndexComplaint','InterventionTypeCause']].pivot_table(
            index='IndexComplaint', 
            columns=['InterventionTypeCause'], 
            aggfunc = nonempty,
            dropna = False).fillna(0)

piv = pd.merge(left = piv,
               right = ComplaintsWithEvents[[
                   'IndexComplaint',
                   'YearComplaint',
                   #'SAP Area - DescriptionComplaint',
                   ]],
               how = 'right',
               left_index = True,
               right_on = 'IndexComplaint')

piv.drop(['NC Incident', 'Interruption', 'NC Planned work'], 
         axis = 'columns',
         inplace=True)

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv[workcols] = piv[workcols].fillna(0)

piv['AnyCause'] = piv[workcols].apply(
    sum, axis=1) > 0

def perc(l):
    return 100*sum(l)/len(l)

aggd = piv[piv['YearComplaint']>=2014][workcols + ['YearComplaint',
#'SAP Area - DescriptionComplaint',
 'AnyCause']].groupby(
    by = ['YearComplaint', 
    #'SAP Area - DescriptionComplaint'
    ]).agg(perc) # Change to sum for raw totals / perc for percentages

#aggd['AnyCause'].unstack().transpose().plot(kind='bar')

fig, ax = plt.subplots()
aggd.transpose().plot(kind='bar', ax=ax)

ax.set_xlabel('Cause type', fontsize = 14)
ax.set_ylabel('% of complaints', fontsize = 14) # Change label for total/%
ax.set_title('Potential causes of complaints', fontsize = 14)
l = ax.get_legend()
l.get_texts()[2].set_text('2016 (to June)')
ax.set_xticklabels(workcols + ['Any Cause'])

#%% Complaints with potential attributions to work types: percentages

def nonempty(l):
    if len(l)>0:
        return 1
    else:
        return 0
    
piv = FullComplaintsEventsOJ[[ #Change for ODI only?
        'IndexComplaint','InterventionTypeCause']].pivot_table(
            index='IndexComplaint', 
            columns=['InterventionTypeCause'], 
            aggfunc = nonempty,
            dropna = False).fillna(0)

piv = pd.merge(left = piv,
               right = ComplaintsWithEvents[[
                   'IndexComplaint',
                   'YearComplaint',
                   'SAP Area - DescriptionComplaint',
                   ]],
               how = 'right',
               left_index = True,
               right_on = 'IndexComplaint')

piv.drop(['NC Incident', 'Interruption', 'NC Planned work'], 
         axis = 'columns',
         inplace=True)

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv[workcols] = piv[workcols].fillna(0)

piv['AnyCause'] = piv[workcols].apply(
    sum, axis=1) > 0

def perc(l):
    return 100*sum(l)/len(l)

aggd = piv[piv['YearComplaint']>=2014][workcols + ['YearComplaint',
'SAP Area - DescriptionComplaint',
 'AnyCause']].groupby(
    by = ['YearComplaint', 
    'SAP Area - DescriptionComplaint'
    ]).agg(sum) # Change to sum for raw totals

    
fig, ax = plt.subplots(figsize = (6,4))
aggd['AnyCause'].unstack().transpose().plot(kind='bar', ax=ax)

ax.set_xlabel('Region', fontsize = 14)
ax.set_ylabel('Total complaints', fontsize = 14) #Change for raw
ax.set_title('Potential causes of complaints by region', fontsize = 14)
leg = ax.get_legend()
leg.set_title('Year')

leg.get_texts()[2].set_text('2016 (to June)')
#ax.set_xticklabels(workcols + ['Any Cause'])


#%% Complaints with potential attributions to work types: by region

def nonempty(l):
    if len(l)>0:
        return 1
    else:
        return 0
    
piv = FullComplaintsEventsOJ[[
        'IndexComplaint','InterventionTypeCause']].pivot_table(
            index='IndexComplaint', 
            columns=['InterventionTypeCause'], 
            aggfunc = nonempty,
            dropna = False).fillna(0)

piv = pd.merge(left = piv,
               right = ComplaintsWithEvents[[
                   'IndexComplaint',
                   'YearComplaint',
                   'SAP Area - DescriptionComplaint',
                   ]],
               how = 'right',
               left_index = True,
               right_on = 'IndexComplaint')

piv.drop(['NC Incident', 'Interruption', 'NC Planned work'], 
         axis = 'columns',
         inplace=True)

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv[workcols] = piv[workcols].fillna(0)

piv['AnyCause'] = piv[workcols].apply(
    sum, axis=1) > 0

def perc(l):
    return 100*sum(l)/len(l)

aggd = piv[piv['YearComplaint']>=2014][workcols + ['YearComplaint',
'SAP Area - DescriptionComplaint',
 'AnyCause']].groupby(
    by = ['YearComplaint', 
    'SAP Area - DescriptionComplaint'
    ]).agg(sum)
    
aggdCentral = aggd.reset_index()
aggdCentral = aggdCentral[aggdCentral['SAP Area - DescriptionComplaint']=='Central']
aggdCentral.index = aggdCentral['YearComplaint']

aggdDerbyshire = aggd.reset_index()
aggdDerbyshire = aggdDerbyshire[aggdDerbyshire['SAP Area - DescriptionComplaint']=='Derbyshire']
aggdDerbyshire.index = aggdDerbyshire['YearComplaint']

#aggd['AnyCause'].unstack().transpose().plot(kind='bar')

fig, ax = plt.subplots(ncols = 2, sharey = True, figsize = (10,4))
aggdCentral[workcols + ['AnyCause']].transpose().plot(kind='bar', ax=ax[0])
aggdDerbyshire[workcols + ['AnyCause']].transpose().plot(kind='bar', ax=ax[1])

for i in range(2):
    ax[i].set_xlabel('Cause type', fontsize = 14)
    leg = ax[i].get_legend()
    leg.set_title('Year')
    leg.get_texts()[2].set_text('2016 (to June)')
    ax[i].set_xticklabels(workcols + ['Any Cause'])

ax[0].set_ylabel('Number of complaints', fontsize = 14)
ax[0].set_title('Central', fontsize = 14)
ax[1].set_title('Derbyshire', fontsize = 14)

#%% Aggregating over the years

def nonempty(l):
    if len(l)>0:
        return 1
    else:
        return 0
    
piv = FullComplaintsEventsOJ[[
        'IndexComplaint','InterventionTypeCause']].pivot_table(
            index='IndexComplaint', 
            columns=['InterventionTypeCause'], 
            aggfunc = nonempty,
            dropna = False).fillna(0)

piv = pd.merge(left = piv,
               right = ComplaintsWithEvents[[
                   'IndexComplaint',
                   'YearComplaint',
                   'SAP Area - DescriptionComplaint',
                   ]],
               how = 'right',
               left_index = True,
               right_on = 'IndexComplaint')

piv.drop(['NC Incident', 'Interruption', 'NC Planned work'], 
         axis = 'columns',
         inplace=True)

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv[workcols] = piv[workcols].fillna(0)

piv['AnyCause'] = piv[workcols].apply(
    sum, axis=1) > 0

def perc(l):
    return 100*sum(l)/len(l)

aggd = piv[piv['YearComplaint']>=2014][workcols + [
'SAP Area - DescriptionComplaint',
 'AnyCause']].groupby(
    by = [
    'SAP Area - DescriptionComplaint'
    ]).agg(sum)

    
fig, ax = plt.subplots()    
aggd[workcols].ix[['Central', 'Derbyshire']].transpose().plot(kind='bar', ax=ax)
#aggd.transpose().plot(kind='bar', ax=ax)

ax.set_xlabel('Cause type', fontsize = 14)
ax.set_ylabel('Total complaints', fontsize = 14) #Change for raw
ax.set_title('Potential causes of complaints: Central/Derbyshire', fontsize = 14)
leg = ax.get_legend()
leg.set_title('SAP Region')






#aggdCentral = aggd.reset_index()
#aggdCentral = aggdCentral[aggdCentral['SAP Area - DescriptionComplaint']=='Central']
#aggdCentral.index = aggdCentral['YearComplaint']
#
#aggdDerbyshire = aggd.reset_index()
#aggdDerbyshire = aggdDerbyshire[aggdDerbyshire['SAP Area - DescriptionComplaint']=='Derbyshire']
#aggdDerbyshire.index = aggdDerbyshire['YearComplaint']

#aggd['AnyCause'].unstack().transpose().plot(kind='bar')

#fig, ax = plt.subplots(ncols = 2, sharey = True, figsize = (10,4))
#aggdCentral[workcols + ['AnyCause']].transpose().plot(kind='bar', ax=ax[0])
#aggdDerbyshire[workcols + ['AnyCause']].transpose().plot(kind='bar', ax=ax[1])
#
#for i in range(2):
#    ax[i].set_xlabel('Cause type', fontsize = 14)
#    leg = ax[i].get_legend()
#    leg.set_title('Year')
#    ax[i].set_xticklabels(workcols + ['Any Cause'])
#
#ax[0].set_ylabel('Number of complaints', fontsize = 14)
#ax[0].set_title('Central', fontsize = 14)
#ax[1].set_title('Derbyshire', fontsize = 14)

#%% By region: work types leading to complaints: percentage
EventsWithComplaints['Year'] = EventsWithComplaints['StartCause'].dt.year
EventsWithComplaints = EventsWithComplaints[EventsWithComplaints['Year']>=2014]

piv = EventsWithComplaints[['InterventionTypeCause', 
                            'SAPRegionCause', 
                            'AtLeastOneComplaint']].pivot_table(
    index = ['SAPRegionCause', 'InterventionTypeCause'],
    columns = ['AtLeastOneComplaint'], 
    aggfunc = len)

piv['%'] = 100*piv[True]/(piv[True]+piv[False])

piv = piv['%'].unstack().transpose()

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
piv.ix[workcols][['Central', 'Derbyshire']].plot(kind = 'bar', ax=ax)
ax.set_ylabel('% followed by complaints', fontsize=14)
ax.set_xlabel('Work type', fontsize=14)
ax.get_legend().set_title('SAP Region')

plt.suptitle('Works and bursts leading to complaints', fontsize=14)

plt.show()

#%% By region: work types leading to complaints: raw

piv = EventsWithComplaints[['InterventionTypeCause', 
                            'SAPRegionCause', 
                            'AtLeastOneComplaint']].pivot_table(
    index = ['SAPRegionCause', 'InterventionTypeCause'],
    columns = ['AtLeastOneComplaint'], 
    aggfunc = len)

piv = piv[True].unstack().transpose()

fig, ax = plt.subplots()
piv[['Central', 'Derbyshire']].plot(kind = 'bar', ax=ax)
ax.set_ylabel('Total followed by complaints', fontsize=14)
ax.set_xlabel('Work type', fontsize=14)

plt.suptitle('Works and bursts leading to complaints', fontsize=14)

plt.show()

##%%
#
#fig, ax = plt.subplots(nrows = 2, ncols = 4, figsize = (16,8), sharey = True, sharex = True)
#
#for i in range(8):
#    aggd[workcols[i]].unstack().transpose().plot(kind='bar', ax=ax.flatten()[i])
#    ax.flatten()[i].set_title(workcols[i])
#
## TODO: Produce more plots like the above.

#%% Forecasting reductions: data

piv1 = EventsWithComplaints[['InterventionTypeCause', 
                            'Year', 
                            'AtLeastOneComplaint']].pivot_table(
    index = [ 'Year', 'InterventionTypeCause'],
    columns = ['AtLeastOneComplaint'], 
    aggfunc = len)

piv1['%'] = 100*piv1[True]/(piv1[True]+piv1[False])

piv1 = piv1['%'].unstack().transpose()

piv = FullComplaintsEventsOJ[[ #Change for ODI only?
        'IndexComplaint','InterventionTypeCause']].pivot_table(
            index='IndexComplaint', 
            columns=['InterventionTypeCause'], 
            aggfunc = nonempty,
            dropna = False).fillna(0)

piv = pd.merge(left = piv,
               right = ComplaintsWithEvents[[
                   'IndexComplaint',
                   'YearComplaint'
                   ]],
               how = 'right',
               left_index = True,
               right_on = 'IndexComplaint')

piv.drop(['NC Incident', 'Interruption', 'NC Planned work'], 
         axis = 'columns',
         inplace=True)

workcols = ['Communication Pipe Burst',
            'Fitting Burst',
            'Supply Pipe Burst',
            'Main Burst',
            'Valve Op',
            'SAP Planned work',
            'SAP Unplanned work',
            'Mains Repair']

piv[workcols] = piv[workcols].fillna(0)

def perc(l):
    return 100*sum(l)/len(l)

aggd = piv[piv['YearComplaint']>=2014][workcols + ['YearComplaint']].groupby(
    by = ['YearComplaint', 
    ]).agg(sum) # Change to sum for raw totals

aggd = aggd.transpose()

EventsWithComplaints.groupby(by  = ['AtLeastOneComplaint',
                                    'InterventionTypeCause',
                                    'Year']).size()

aggd.transpose()
