"""
Created on Thu Mar 29 12:48:21 2018

@author: jbuckley
"""

import os
import pickle
import importlib
import datetime
import pandas as pd
import numpy as np
from keen.client import KeenClient
import random
from multiprocessing.dummy import Pool
from collections import namedtuple

# TIMEFRAME FUNCTION
def timeframe_gen(start, end, hour_interval=24, tz='US/Eastern'):
    """
    creates timeframe for use in making Keen API calls
    + args
    start - start date (str - '2017-08-04'); inclusive
    end - end date (str - '2017-12-04'); inclusive; it will always include
        and never exceed this date
    + kwargs:
    hour_interval - interval for breaking up start, end tuple
    tz - timezone, default to US/Eastern

    returns:
        List of tuples; tuple - (start, end)
    """
    freq = str(hour_interval) + 'H'
    end = pd.to_datetime(end)
    end = end + datetime.timedelta(1)
    start_dates = pd.date_range(start, end, freq=freq, tz=tz)
    start_dates = start_dates.tz_convert('UTC')
    end_dates = start_dates + pd.Timedelta(hour_interval, unit='h')


    strftime = datetime.datetime.strftime
    time_format = '%Y-%m-%dT%H:%M:%S.000Z'

    start_times = [strftime(i, time_format) for i in start_dates]
    end_times = [strftime(i, time_format) for i in end_dates]

    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]

    return timeframe[:-1]

# Email Data Functions
def email_delivered(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_delivered'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('category', 'marketing_campaign_info.name', 'marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    #print(start+": Done", end=' | ')
    return df

def email_processed(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_processed'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    #print(start+": Done", end=' | ')
    return df

def email_opened(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_open'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    return df

def unique_opens(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_open'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('email', 'marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    return df

def new_unique_opens(keen, campaign_id, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_open'

    target_property = 'email'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id')

    property_name1 = 'marketing_campaign_info.id'
    operator1 = 'eq'
    property_value1 = campaign_id

    filters = [{"property_name":property_name1,"operator":operator1,"property_value":property_value1}]

    data = keen.count_unique(event,
                    timeframe=timeframe,
                    target_property=target_property,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    return df

def email_unsubs(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_unsubscribe'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    #print(start+": Done", end=' | ')
    return df

def email_bounces(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_bounce'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    #print(start+": Done", end=' | ')
    return df

def email_delivered_overall(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_delivered'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id','marketing_campaign_info.name')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    print(str(start)[:10])
    return df

def email_processed_overall(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_processed'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('marketing_campaign_info.id','marketing_campaign_info.name')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    print(str(start)[:10])
    return df

def users_opened(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_open'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('keen.timestamp', 'email','marketing_campaign_info.id','campaign')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = start
    print(start+": Done", end=' | ')
    return df

def clean_delivered(data):
    """
    Cleans overall delivered dataframes
    """
    df_del = pd.concat(data)
    df_del = df_del.groupby(['marketing_campaign_info.id','marketing_campaign_info.name']).agg({'result':sum,'start':'first'})
    df_del = df_del.reset_index().sort_values('start')
    df_del = df_del.rename(columns={'marketing_campaign_info.name':'campaign_name'})
    df_del = df_del.rename(columns={'result':'delivered'})
    df_del = df_del[(df_del.campaign_name.str.contains("test") == False)&
                    (df_del.campaign_name.str.contains("Copy") == False)&
                   (df_del.campaign_name.str.contains("Testing") == False)].copy()
    return(df_del)

def clean_processed(data1):
    """
    Cleans overall processed dataframes
    """
    df_proc = pd.concat(data1)
    df_proc = df_proc.groupby(['marketing_campaign_info.id','marketing_campaign_info.name']).agg({'result':sum,'start':'first'})
    df_proc = df_proc.reset_index().sort_values('start')
    df_proc = df_proc.rename(columns={'marketing_campaign_info.name':'campaign_name'})
    df_proc = df_proc.rename(columns={'result':'processed'})
    df_proc = df_proc[(df_proc.campaign_name.str.contains("test") == False)&
                    (df_proc.campaign_name.str.contains("Copy") == False)&
                   (df_proc.campaign_name.str.contains("Testing") == False)].copy()
    return(df_proc)

def stringer(a_list):
    try:
        return(a_list[0])
    except:
        pass


def easy_date(hour_date):
    """
    Removes hours from datetime
    """
    return(hour_date.date())

def subs_clean(data):
    """
    Cleans subscribers dataframe
    """
    new_df = pd.concat(data)
    new_df = new_df.groupby('marketing_campaign_info.id',as_index=False)['result'].sum()
    new_df = new_df.rename(columns={'result':'Subscribers'})
    return(new_df)

def del_clean(df4):
    """
    Cleans delivered DataFrame
    """
    df4['email_cat'] = df4['category'].apply(stringer)
    df_del = df4.groupby(['email_cat','start','marketing_campaign_info.id','marketing_campaign_info.name'],as_index=False)['result'].sum()
    df_del['date'] = df_del['start'].apply(easy_date)
    df_del = df_del.groupby(['email_cat','marketing_campaign_info.name','marketing_campaign_info.id','date'],as_index=False)['result'].sum()
    df_del = df_del.rename(columns={'result':'delivered'})
    df_del = df_del.groupby(['email_cat','marketing_campaign_info.name','marketing_campaign_info.id'],as_index=False).agg({'delivered':sum,
                                                                                   'date': 'min'})
    df_del = df_del.sort_values('delivered',ascending=False)
    return(df_del)

def open_clean(df5):
    """
    Cleans opens DataFrame
    """
    df_open = df5.copy()
    df_open['date'] = df_open['start'].apply(easy_date)
    df_open = df_open.groupby(['marketing_campaign_info.id'],as_index=False).agg({'result':sum})
    df_open = df_open.rename(columns={'result':'opens'})
    df_open = df_open.sort_values('opens',ascending=False)
    return(df_open)

def unique_clean(df6):
    """
    Cleans unique opens DataFrame
    """

    df_unq_test = df6.copy()
    df_unq_test['date'] = df_unq_test['start'].apply(easy_date)
    df_ubyemail = df_unq_test.groupby('marketing_campaign_info.id')['email'].nunique()
    df_email_uniques = pd.DataFrame(df_ubyemail).reset_index().sort_values('email',ascending=False)
    df_email_uniques = df_email_uniques.rename(columns={'email':'uniques'})
    return(df_email_uniques)

def unsub_clean(df1):
    """
    Cleans unsubscribe DataFrame
    """
    df_unsub = df1.copy()
    df_unsub = df_unsub.groupby('marketing_campaign_info.id')['result'].sum().reset_index().sort_values('result',ascending=False)
    df_unsub = df_unsub.rename(columns={'result':'Unsubscribes'})
    return(df_unsub)

def bounce_clean(df2):
    """
    Cleans bounces DataFrame
    """
    df_bounce = df2.copy()
    df_bounce = df_bounce.groupby('marketing_campaign_info.id')['result'].sum().reset_index().sort_values('result',ascending=False)
    df_bounce = df_bounce.rename(columns={'result':'Bounces'})
    return(df_bounce)

def email_classify(category):
    """
    takes category and returns email
    """
    if "daily-brief" in category:
        return("Daily Brief")
    elif "quartz-obsession" in category:
        return("Obsession")
    elif "quartzy" in category:
        return("Quartzy")
    elif "quartz-index" in category:
        return("Index")
    elif "africa-weekly-brief" in category:
        return("Africa Weekly")
    else:
        pass

def region_classify(category):
    """
    takes category and returns region
    """
    if "americas" in category:
        return("Americas")
    elif "asia" in category:
        return("Asia")
    elif "europeandafrica" in category:
        return("EMEA")
    elif "–\xa0uk" in category:
        return("UK")
    elif "- uk" in category:
        return("UK")
    elif "–\xa0us" in category:
        return("US")
    elif "–\xa0non-us" in category:
        return("Non-US")
    elif "- us" in category:
        return("US")
    elif "- non-us" in category:
        return("Non-US")
    else:
        return("Global")
