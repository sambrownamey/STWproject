# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 07:52:13 2017

@author: Sam
"""

import pandas as pd

picklepath = 'PickledData/BurstsNetbaseDF.pkl'

def read_bursts_netbase():
    infile = 'BurstsNetbaseTrimmed.csv'
    directory = 'S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/'
    BurstsNetbaseDF = pd.read_csv(directory + infile,
                                  parse_dates = ['Date Reported', 'Date Completed'],
                                  infer_datetime_format = True, 
                                  dayfirst = True,
                                  encoding = 'latin-1')                           
    BurstsNetbaseDF['OrderAndOp'] = BurstsNetbaseDF['External ID1'].apply(
        lambda s: s.replace("-","/"))    
    BurstsNetbaseDF['Order'] = BurstsNetbaseDF['External ID1'].apply(
        lambda s: int(s.split("-")[0]))
    BurstsNetbaseDF['Operation'] = BurstsNetbaseDF['External ID1'].apply(
        lambda s: int(s.split("-")[1]))
    BurstsNetbaseDF['Postcode'] = BurstsNetbaseDF['Address - Postcode'][
        BurstsNetbaseDF['Address - Postcode'].notnull()].apply(
            lambda s: s.upper().replace(" ",""))
    return BurstsNetbaseDF
    
if __name__=='__main__':
    BurstsNetbaseDF = read_bursts_netbase()
    BurstsNetbaseDF.to_pickle(picklepath)
else:
    try:
        BurstsNetbaseDF = pd.read_pickle(picklepath)
    except FileNotFoundError as e:
        print(e, "Error, could not find pickle file. Try running the bursts reader script directly")
        raise(e)
# Manual check: all the mins and maxes match: i.e. there's only one reporting
# date for each order number. This is good!

#BurstsGrouped = pd.DataFrame(
#    BurstsNetbaseDF.groupby(
#        by = ['Order'], 
#        as_index = False)['Date Reported'].agg([min, max])
#BurstsGrouped['Check'] = BurstsGrouped['min']==BurstsGrouped['max']   

def makelist(l):
    uniques = set([str(x) for x in l])
    li = list(uniques)
    li.sort()
    return ",".join(li)
    
def li(l):
    return tuple(l)
    
BurstsGrouped = pd.DataFrame(
    BurstsNetbaseDF.groupby(
        by = ['Order', 'Burst Type', 'Date Reported', 'Postcode', 'Leakage Type'], 
        as_index = False)['Operation'].agg([li])
    ).reset_index()
    
BurstsGrouped.rename(columns={'li':'Operations'}, inplace = True)


