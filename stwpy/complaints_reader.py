# -*- coding: utf-8 -*-
"""
This script reads the complaints data

@author: 142796
"""

import pandas as pd
import numpy as np
from stwpy.dmautils import DMAsNetbase

PostcodeAcornPath = "S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/00. MI Analysis/Data/Data Linking Docs/Postcode DMA_ACORN WQ data.csv"
PickledComplaintsDFPath = 'PickledData/ComplaintsDF.pkl'

directory = "S:/00_Projects/CO00690090_STW WQ Analytics/04_Background_Documents_Data/CleanedData/"
infile = "AllComplaintsClean.csv"

# Demographics

PostcodeAcorn = pd.read_csv(PostcodeAcornPath)
PostcodeAcorn.ID_DMA_COD = PostcodeAcorn.ID_DMA_COD.apply(
        lambda i: str(i).zfill(5))
PostcodeAcorn['Key'] = PostcodeAcorn['Key'].apply(
        lambda s: s.replace(" ",""))

acorn5 = ["C1", "C2", "C3", "C4", "C5"] # Acorn categories
acorn17 = list("ABCDEFGHIJKLMNOPQ") # Acorn groups
acorn59 = range(1,60) # Acorn types

acorn_dict = { "C1":{"A":[1,2,3],
                     "B":[4,5,6,7,8,9],
                     "C":[10,11,12,13]},
               "C2":{"D":[14,15,16,17],
                     "E":[18,19,20]},
               "C3":{"F":[21,22,23],
                     "G":[24,25,26],
                     "H":[27,28,29],
                     "I":[30,31],
                     "J":[32,33]},
               "C4":{"K":[34,35,36],
                     "L":[37,38,39,40],
                     "M":[41,42,43,44],
                     "N":[45,46,47,48]},
               "C5":{"O":[49,50,51],
                     "P":[52,53,54,55,56],
                     "Q":[57,58,59]}
              }

acorn17_dict = {}
for d in acorn_dict.values():
    acorn17_dict.update(d)    
a59to17 = {n:l for l in acorn17 for n in acorn17_dict[l]}

for i in range(60,63):           
    a59to17[i] = 'Business'
    
acorn5_dict = {}
for d in acorn5:
    result = [i for j in acorn_dict[d].values() for i in j]
    acorn5_dict[d] = result  

a59to5 = {n:l for l in acorn5 for n in acorn5_dict[l]}

for i in range(60,63):           
    a59to5[i] = 'Business'

postcode_to_acorn_dict = PostcodeAcorn[['Key', 'ACORN']].set_index('Key')
padict = postcode_to_acorn_dict.to_dict()['ACORN']

PostcodeAcorn['Acorn17'] = PostcodeAcorn['Key'].apply(lambda i: a59to17.get(padict.get(i)))
PostcodeAcorn['Acorn5'] = PostcodeAcorn['Key'].apply(lambda i: a59to5.get(padict.get(i)))

if __name__=="__main__":
    
    def datetime_parser(d,t):
        try:
            return pd.to_datetime(d + " " + t, format = "%d/%m/%Y %H:%M:%S")
        except:
            return pd.NaT
    
    def date_parser(d):
        if pd.isnull(d):
            return pd.NaT
        try:
            return pd.to_datetime(d, format = "%d/%m/%Y")
        except:
            return pd.NaT            
    
    #Import complaints data
    ComplaintsDF = pd.read_csv(
        directory + infile,
        parse_dates = {'DateTime': ['Date', 'Time']},
        keep_date_col=True,
        infer_datetime_format = True,
        date_parser = datetime_parser,
        dayfirst = True,
        encoding = "latin-1")
    
    ComplaintsDF['Date'] = ComplaintsDF['Date'].apply(date_parser).dt.date
    
    ComplaintsDF['Incident Related'] = ComplaintsDF['Incident Related']=='Y'
    ComplaintsDF = ComplaintsDF.drop('Unnamed: 0', axis = 1)
    
    
        
    ComplaintsDF['X'] = ComplaintsDF['X'].replace(0, np.nan)
    ComplaintsDF['Y'] = ComplaintsDF['Y'].replace(0, np.nan)
    
    complaints_pivot = ComplaintsDF.pivot_table(index = 'DMA', 
                                                columns = 'Date', 
                                                values = 'ComplaintRef', 
                                                aggfunc = len, 
                                                fill_value = 0)
    
    def _get_entry(row, table):
        try:
            return table.ix[row.DMA, row.Date]
        except:
            return 1
    
    ComplaintsDF['DailyDMAClusterSize'] = ComplaintsDF[['Date', 'DMA']].apply(
        lambda row: _get_entry(row, complaints_pivot), axis = 1)
      
    def consecutive_sums(l):
        newlist1 = []
        partial_list = [l[0]]
        for i in range(1, len(l)):
            if (l[i]==0) != (l[i-1]==0):
                newlist1.append(partial_list)
                partial_list = [l[i]]
            else:
                partial_list.append(l[i])
        newlist1.append(partial_list)
        newlist2 = []
        for pl in newlist1:
            newlist2.extend([np.sum(pl)]*len(pl))      
        return newlist2
        
    # TODO: Add a "cluster ID" to the above to help with catalogueing of complaint
    # clusters, and to use in attributing them to causes
        
    consecutive_day_cluster_sizes = complaints_pivot.apply(consecutive_sums, axis= 1)            
    
    ComplaintsDF['ConsecutiveDaysDMAClusterSize'] = ComplaintsDF[['Date', 'DMA']].apply(
        lambda row: _get_entry(row, consecutive_day_cluster_sizes), axis = 1)
    
    # Demographics columns
    
    ComplaintsDF['Acorn59'] = ComplaintsDF['Postcode'].apply(lambda i: padict.get(i))
    ComplaintsDF['Acorn17'] = ComplaintsDF['Postcode'].apply(lambda i: a59to17.get(padict.get(i)))
    ComplaintsDF['Acorn5'] = ComplaintsDF['Postcode'].apply(lambda i: a59to5.get(padict.get(i)))
    

    ComplaintsDF['Time'] = ComplaintsDF[
            'DateTime'].dt.time
    
    ComplaintsDF['Hour'] = ComplaintsDF[
            'DateTime'].dt.hour

    def time_to_decimal_hour(ts):
        if pd.notnull(ts):
            return ts.hour + ts.minute/60 + ts.second/3600
        else:
            return None
    
    ComplaintsDF['DecimalHour'] = ComplaintsDF[
        'Time'].apply(time_to_decimal_hour)
    
    ComplaintsDF['OrdinalDateTime'] = ComplaintsDF[
        'DateTime'].apply(lambda z: z.toordinal() if pd.notnull(z) else None) \
            + ComplaintsDF['DecimalHour']/24      
        
    ComplaintsDF['DayOfWeek'] = ComplaintsDF[
        'DateTime'].apply(lambda ts: ts.weekday() if pd.notnull(ts) else None)
    ComplaintsDF.to_pickle(PickledComplaintsDFPath)
 
else:
    try:
        ComplaintsDF = pd.read_pickle(PickledComplaintsDFPath)
    except FileNotFoundError as e:
        print("Error: pickle file not found. Try running complaints_reader.py.")
        raise e




    
Acorn5Info = pd.DataFrame(PostcodeAcorn.groupby(by = 'Acorn5')['TOTALHOUSEHOLDS'].sum())
Acorn5Info['TotalComplaints'] = ComplaintsDF.groupby(by = 'Acorn5').size()
Acorn5Info['ComplaintsPer100Households'] = 100*Acorn5Info['TotalComplaints']/Acorn5Info['TOTALHOUSEHOLDS']
#Acorn5Info.ix[acorn5]['ComplaintsPer100Households'].plot.bar()

Acorn17Info = pd.DataFrame(PostcodeAcorn.groupby(by = 'Acorn17')['TOTALHOUSEHOLDS'].sum())
Acorn17Info['TotalComplaints'] = ComplaintsDF.groupby(by = 'Acorn17').size()
Acorn17Info['ComplaintsPer100Households'] = 100*Acorn17Info['TotalComplaints']/Acorn17Info['TOTALHOUSEHOLDS']
#Acorn17Info.ix[acorn17]['ComplaintsPer100Households'].plot.bar()

Acorn59Info = pd.DataFrame(PostcodeAcorn.groupby(by = 'ACORN')['TOTALHOUSEHOLDS'].sum())
Acorn59Info['TotalComplaints'] = ComplaintsDF.groupby(by = 'Acorn59').size()
Acorn59Info['ComplaintsPer100Households'] = 100*Acorn59Info['TotalComplaints']/Acorn59Info['TOTALHOUSEHOLDS']
#Acorn59Info.ix[acorn59]['ComplaintsPer100Households'].plot.bar()

Acorn5Info['TotalDiscolComplaints'] = ComplaintsDF[ComplaintsDF['Translated']=='Appearance'].groupby(by = 'Acorn5').size()
Acorn5Info['DiscolComplaintsPer100Households'] = 100*Acorn5Info['TotalDiscolComplaints']/Acorn5Info['TOTALHOUSEHOLDS']

Acorn17Info['TotalDiscolComplaints'] = ComplaintsDF[ComplaintsDF['Translated']=='Appearance'].groupby(by = 'Acorn5').size()
Acorn17Info['DiscolComplaintsPer100Households'] = 100*Acorn17Info['TotalDiscolComplaints']/Acorn17Info['TOTALHOUSEHOLDS']

Acorn59Info['TotalDiscolComplaints'] = ComplaintsDF[ComplaintsDF['Translated']=='Appearance'].groupby(by = 'Acorn5').size()
Acorn59Info['DiscolComplaintsPer100Households'] = 100*Acorn59Info['TotalDiscolComplaints']/Acorn59Info['TOTALHOUSEHOLDS']



      
#DMAsNetbase = pd.read_csv('DMAsNetbase.csv', 
#                          encoding = 'latin-1', 
#                          dtype = {'Reference':str} )

ComplaintsDFWithRegions = pd.merge(
    ComplaintsDF, 
    DMAsNetbase, 
    how = 'left',
    left_on = 'DMA',
    right_on = 'Reference')

#m, n = complaints_pivot.values.shape
#
#A = np.array([[1 if j in [i, i+1, i+2] else 0 for j in range(n)] for i in range(n-2)]).T
#
#
#complaints_partial_sums = np.dot(complaints_pivot.values,A)
#clusters_pivot = pd.DataFrame(data = complaints_partial_sums >= 3, 
#                              index = complaints_pivot.index, 
#                              columns = complaints_pivot.columns[:-2] )
