#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
title               :vmt_aggregation.py
description         :group sensor data downloaded using datagrab.py
author              :sbuchhorn
created             :20 Dec 2018
revisions:
20 Dec 2018 - round AADT to nearest 100, join AADT with lookup file and save
16 Jan 2019 - take out zeros in AADT calculation, take out blanks in lookup file
9 Jan 2020 - update for 2019 aggregation, including separate processing for DST months
21 Jan 2020 - fix the aadt calculation (average of averages, not just average over year)
"""

# import statements
import dask.dataframe as dd
import time
import pandas as pd
import os


# set pathways
# datapath is the location of the downloaded sensor data, savepath is the location to save the hourly groupby, and
#   agg_savepath is the location for the groupbys above the hourly level and the updated lookup table
# make sure no other files are in the savepath (or change code for that portion below)
datapath = "S:/AdminGroups/PerformanceProgramming/RegionalTransportationPerformanceMeasures/KeyDatasets/" \
       "IDOTExpresswayAtlasData/2019/raw/"
savepath = "S:/AdminGroups/PerformanceProgramming/RegionalTransportationPerformanceMeasures/KeyDatasets/" \
       "IDOTExpresswayAtlasData/2019/hour/"
agg_savepath = "S:/AdminGroups/PerformanceProgramming/RegionalTransportationPerformanceMeasures/KeyDatasets/" \
       "IDOTExpresswayAtlasData/2019/2019_grouped_files/"
# detectoridcolname is the column in the lookup file with the detectorid
lookup_file = "S:/AdminGroups/PerformanceProgramming/RegionalTransportationPerformanceMeasures/KeyDatasets/IDOTExpresswayAtlasData/2018/working/rampid_2018aadt_working.csv"
detectoridcolname = "detectorid2"

# loop to aggregate to hourly level
# this takes around 40-60 min for each month
for year_month in ['2019_12']:
    print("Starting " + year_month)
    start_time = time.time()
    df = dd.read_csv(datapath + year_month + ".csv",
        names=["DetectorId", "TimeStamp", "volume", "count"],
        dtype={'volume': 'float64'})
    df['datetest'] = df.TimeStamp.astype('datetime64')
    df.datetest = df.datetest.dt.tz_localize(tz='UTC')
    df.datetest = df.datetest.dt.tz_convert('US/Central')
    df['date'] = df.datetest.dt.date
    df['dow'] = df.datetest.dt.dayofweek
    df['hour'] = df.datetest.dt.hour
    df['DetectorId1'] = df['DetectorId']
    hour = df.groupby(['date', 'dow', 'hour', 'DetectorId']).agg({'volume': 'sum', 'DetectorId1': 'count'})
    hour.to_csv(savepath + year_month + "_hour_*.csv")
    print("Finished " + year_month + " in {}".format(time.time() - start_time))

# DAYLIGHT SAVINGS TIME - file is saved in UTC
for year_month in ['2019_03', '2019_11']:
    print("Starting " + year_month)
    start_time = time.time()
    df = dd.read_csv(datapath + year_month + ".csv",
        names=["DetectorId", "TimeStamp", "volume", "count"],
        dtype={'volume': 'float64'})
    df['datetest'] = dd.to_datetime(df.TimeStamp, utc='true', errors='ignore')
    df['date'] = df.datetest.dt.date
    df['dow'] = df.datetest.dt.dayofweek
    df['hour'] = df.datetest.dt.hour
    df['DetectorId1'] = df['DetectorId']
    hour = df.groupby(['date', 'dow', 'hour', 'DetectorId']).agg({'volume': 'sum', 'DetectorId1': 'count'})
    hour.to_csv(savepath + year_month + "_hour_UTC_*.csv")
    print("Finished " + year_month + " in {}".format(time.time() - start_time))

for year_month in ['2019_11']:
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H')
    month = pd.read_csv(savepath + year_month + '_hour_UTC_0.csv', parse_dates=[['date','hour']], date_parser=dateparse)
    month['date_hour'] = month.date_hour.dt.tz_localize(tz='UTC')
    month['central'] = month.date_hour.dt.tz_convert('US/Central')
    month['date'] = month.central.dt.date
    month['dow'] = month.central.dt.dayofweek
    month['hour'] = month.central.dt.hour
    # now we have all the columns, in central time!  save to file
    month.to_csv(savepath + year_month + '_hour_0.csv')


# read in all months as pandas dataframes
dflist = []
for file in os.listdir(savepath):
    if 'UTC' not in file:
        print(file)
        df = pd.read_csv(savepath + file)
        dflist.append(df)

# merge all months
combined = pd.concat(dflist)

# testing
hour = combined.groupby(['date','dow','hour','DetectorId']).agg({'volume':'sum','DetectorId1':'sum'})
hour.to_csv(agg_savepath + "hour_dow_date.csv")
hour = pd.read_csv(agg_savepath + "hour_dow_date.csv")

# group by date and dow, save
dow = combined.groupby(['date','dow','DetectorId']).agg({'volume':'sum','DetectorId1':'sum'})
dow.to_csv(agg_savepath + "dow_date.csv")

# read back in to get all columns after the groupby (there are other ways to accomplish this too)
dow = pd.read_csv(agg_savepath + "dow_date.csv")

# make a month column
dow.date = dow.date.astype('datetime64')
dow['month'] = dow.date.dt.month

# get rid of zeros (zero volume for the whole day), then get average daily volume by month, dow, save
dow0 = dow[dow.volume != 0]
monthdowavg = dow0.groupby(['month','dow','DetectorId']).agg({'volume':'mean','DetectorId1':'sum'})
monthdowavg.to_csv(agg_savepath + "monthdowavg0.csv")

# sum volume by month and dow, save
monthdowsum = dow0.groupby(['month','dow','DetectorId']).agg({'volume':'sum','DetectorId1':'sum'})
monthdowsum.to_csv(agg_savepath + "monthdowsum0.csv")

# average over all hours and dates by DOW
dowonly = monthdowavg.groupby(['DetectorId','dow']).agg({'volume':'mean','DetectorId1':'sum'})
dowonly.to_csv(agg_savepath + "dowonly_v2.csv")

# average over all DOW, round to nearest 100
aadt = dowonly.groupby('DetectorId').agg({'volume':'mean'})
aadt['volume'] = aadt['volume']/100
aadt['volume'] = aadt['volume'].round(0)
aadt['volume'] = aadt['volume'] * 100
aadt.to_csv(agg_savepath + "aadt_no0_v2.csv")

# populate lookup file with aadt
lookup = pd.read_csv(lookup_file)
aadt.rename({'volume':'aadt19'},axis=1,inplace=True)
l_df = lookup.merge(aadt,how='left',left_on=detectoridcolname,right_on='DetectorId')
l_df = l_df[~l_df.aadt19.isnull()]
l_df.to_csv(agg_savepath + "lookup_aadt_no0_blanks_v2.csv")
