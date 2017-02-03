# -*- coding: utf-8 -*-
"""
Created on Tue Dec 20 09:59:18 2016

@author: 142796
"""

import pandas as pd

datapath = "H:\Documents\STW\TelemetryScraping\SplitData"

def get_logger_data(logger, suffix = 0):
    print("Getting data for logger", logger)
    dataframe = pd.read_csv(datapath + "\\" + logger + "-" + str(suffix) + ".csv",
                                parse_dates = ['DateTime'],
                                date_parser = lambda x: pd.to_datetime(x, format = '%d-%m-%Y %H:%M'))
    
    #dataframe.columns = ['DateTime', 'Reading']
    dataframe = dataframe.set_index('DateTime')
    
    dataframe['DayOfWeek'] = dataframe.index.dayofweek
    dataframe['MinOfDay'] = dataframe.index.minute + 60*dataframe.index.hour
    return dataframe