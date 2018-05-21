import os
import pickle
import importlib
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from keen.client import KeenClient
os.chdir('/Users/jbuckley/Python Jupyter/Product')
import email_data_scripts as eds
importlib.reload(eds)
from multiprocessing.dummy import Pool


# Overall Data by day
def overall_email_data_update(email_data,directory='/Users/jbuckley/Python Jupyter/Product',xlsx_file='all_email_overall_data.xlsx'):
    """
    - Takes existing email_data df and appends latest email data through yesterday
    - Sends data to excel file
    """
    email_min = min(email_data['date'])
    email_max = max(email_data['date'])
    yesterday = datetime.datetime.now()
    yesterday = yesterday - datetime.timedelta(1)
    yesterday = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    print("Old Max. Date: "+ str(email_max))

    if yesterday != datetime.datetime.strftime(email_max, '%Y-%m-%d'):
        new_start = datetime.datetime.strftime(email_max + datetime.timedelta(days=1),'%Y-%m-%d')
        new_end = yesterday
        print("Start: " + new_start)
        print("End: " + new_end)
        hour_interval = 24

        timeframe = eds.timeframe_gen(new_start, new_end, hour_interval=24, tz='US/Eastern')
        pool_iter = [(keen, i[0], i[1]) for i in timeframe]

        pool = Pool(8)
        data = pool.starmap(eds.email_processed, pool_iter)
        print("Subscribers: Done")
        data_4 = pool.starmap(eds.email_delivered, pool_iter)
        print("Delivered: Done")
        data_5 = pool.starmap(eds.email_opened, pool_iter)
        print("Opened: Done")
        data_6 = pool.starmap(eds.unique_opens, pool_iter)
        print("Uniques: Done")
        data_1 = pool.starmap(eds.email_unsubs, pool_iter)
        print("Unsubscribes: Done")
        data_2 = pool.starmap(eds.email_bounces, pool_iter)
        print("Bounces: Done")
        pool.close()
        pool.join()

        # Subscribers
            # Not necessary because function does it
        # Delivered
        df4 = pd.concat(data_4)
        df4 = df4.reset_index(drop=True)
        # Opened
        df5 = pd.concat(data_5)
        df5 = df5.reset_index(drop=True)
        # Unique Opens
        df6 = pd.concat(data_6)
        df6 = df6.reset_index(drop=True)
        # Unsubscribes
        df1 = pd.concat(data_1)
        df1 = df1.reset_index(drop=True)
        # Bounced emails
        df2 = pd.concat(data_2)
        df2 = df2.reset_index(drop=True)

        # Clean new datasets
        df_subs = eds.subs_clean(data)
        df_del = eds.del_clean(df4)
        df_open = eds.open_clean(df5)
        df_email_uniques = eds.unique_clean(df6)
        df_unsub = eds.unsub_clean(df1)
        df_bounce = eds.bounce_clean(df2)

        # Merge & clean new datasets
        dft = pd.merge(df_del,df_open,on=['marketing_campaign_info.id'],how='left')
        dft = pd.merge(dft,df_subs,on=['marketing_campaign_info.id'],how='left')
        dft = pd.merge(dft,df_email_uniques,on=['marketing_campaign_info.id'],how='left')
        dft = pd.merge(dft,df_unsub,on=['marketing_campaign_info.id'],how='left')
        dft = pd.merge(dft,df_bounce,on=['marketing_campaign_info.id'],how='left')

        dft = dft.groupby(['email_cat','marketing_campaign_info.name','marketing_campaign_info.id'],as_index=False).agg({'date': 'min',
                                                                                                       'Subscribers':sum,
                                                                                                       'delivered':sum,
                                                                                                       'opens':sum,
                                                                                                       'uniques':sum,
                                                                                                       'Unsubscribes':sum,
                                                                                                       'Bounces':sum})


        dft['email'] = dft['email_cat'].apply(eds.email_classify)
        dft['region'] = dft['email_cat'].apply(eds.region_classify)
        dft['Open Rate'] = dft['opens'] / dft['delivered']
        dft['Unique Open Rate'] = dft['uniques'] / dft['delivered']
        dft['region'] = dft['region'].fillna('Global')
        dft = dft[dft['email_cat']!='category-test'].copy()

        # Append to existing email data
        email_data = email_data.append(dft)
        email_data = email_data.groupby(['email_cat','marketing_campaign_info.name','marketing_campaign_info.id'],as_index=False).agg({'date': 'min',
                                                                                                       'Subscribers':sum,
                                                                                                       'delivered':sum,
                                                                                                       'opens':sum,
                                                                                                       'uniques':sum,
                                                                                                       'Unsubscribes':sum,
                                                                                                       'Bounces':sum})


        email_data['email'] = email_data['email_cat'].apply(eds.email_classify)
        email_data['region'] = email_data['email_cat'].apply(eds.region_classify)
        email_data['Open Rate'] = email_data['opens'] / email_data['delivered']
        email_data['Unique Open Rate'] = email_data['uniques'] / email_data['delivered']
        email_data['region'] = email_data['region'].fillna('Global')

        os.chdir(directory)
        email_data = email_data.sort_values('date')
        email_data['email_cat'] = email_data['email_cat'].str.replace('–\xa0',"- ")
        email_data = email_data[email_data['Subscribers']!=0]
        email_data.to_excel(xlsx_file)
        print('Updated and put into Excel through ' + new_end)
        return(email_data)
    else:
        print("Data all good through " + datetime.datetime.strftime(email_max, '%Y-%m-%d'))
        return(email_data)

## High level data by month functions
def high_level_monthly(email_data):
    """ Takes Overall email data and returns high level stats"""

    all_lists = set(email_data['email_cat'])

    test_data = email_data.copy()
    test_data['year'] = pd.DatetimeIndex(test_data['date']).year
    test_data['month'] = pd.DatetimeIndex(test_data['date']).month

    empty_list=[]
    for email in all_lists:
        dft = test_data[test_data['email_cat']==email].copy()
        dft = dft.groupby(['email_cat','email','region','year','month'],as_index=False).agg({'Subscribers':np.median,
                                                                            'Unique Open Rate':np.median,
                                                                            'Open Rate':np.median,
                                                                            'delivered':np.median,
                                                                            'opens':np.median,
                                                                            'uniques':np.median,
                                                                            'Unsubscribes':sum,
                                                                            'Bounces':sum})
        empty_list.append(dft)


    growth_list = []
    for i in empty_list:
        new_list=[]
        diff_list=[]
        for x in range(0,len(i)):
            if x ==0:
                new_list.append(None)
                diff_list.append(None)
            else:
                new = i['Subscribers'].iloc[x]
                old = i['Subscribers'].iloc[(x-1)]
                growth = (new/old-1)
                change = new-old
                diff_list.append(change)
                new_list.append(growth)
        i['Monthly_growth_rate'] = new_list
        i['Monthly_Subscriber_Change'] = diff_list

    high_level_data = pd.concat(empty_list)
    high_level_data['Date'] = high_level_data['month'].astype(str) + " - " + high_level_data['year'].astype(str)
    return(high_level_data)


def global_data(regional_high_level):
    """ Takes regional high level data DF and returns global data for Daily Brief, Obsession, and Quartzy"""

    global_data = regional_high_level.groupby(['email','year','month',],as_index=False).agg({'Subscribers':sum,
                                                                                'delivered':sum,
                                                                                'opens':sum,
                                                                                'uniques':sum,
                                                                                'Unsubscribes':sum,
                                                                                'Bounces':sum})
    global_data = global_data[(global_data['email']!= 'Africa Weekly')& (global_data['email']!= 'Index')]
    global_data['email_cat'] = global_data['email'] + " - Overall"
    global_data['region'] = 'Global'
    global_data['Unique Open Rate'] = global_data['uniques'] / global_data['delivered']
    global_data['Open Rate'] = global_data['opens'] / global_data['delivered']

    # Calculate growth/differences
    growth_list = []
    pd_list=[]
    for i in set(global_data['email']):
        dft = global_data[global_data['email'] == i].copy()
        new_list=[]
        diff_list=[]
        for x in range(0,len(dft)):
            if x ==0:
                new_list.append(None)
                diff_list.append(None)
            else:
                new = dft['Subscribers'].iloc[x]
                old = dft['Subscribers'].iloc[(x-1)]
                growth = (new/old-1)
                change = new-old
                diff_list.append(change)
                new_list.append(growth)
        dft['Monthly_growth_rate'] = new_list
        dft['Monthly_Subscriber_Change'] = diff_list
        pd_list.append(dft)

    global_overall = pd.concat(pd_list)
    global_overall['Date'] = global_overall['month'].astype(str) + " - " + global_overall['year'].astype(str)
    global_overall = global_overall[regional_high_level.columns]
    return(global_overall)


def combine_high_level_data(regional_data,global_data,directory='/Users/jbuckley/Python Jupyter/Product',xlsx_name='EMAIL_high_level_data.xlsx'):
    """Combines 2 DataFrames, writes to excel file, returns new DF"""
    new_high_level = pd.concat([regional_data,global_data])
    os.chdir(directory)
    new_high_level.to_excel(xlsx_name)
    return(new_high_level)


## Click functions
def link_clicks(keen, start, end):
    """
    used to find the article_tags in articles
    """

    event = 'email_click'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('keen.timestamp', 'url','marketing_campaign_info.id','geo_info.country','url_offset.index')

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=None)

    df = pd.DataFrame(data)
    df['start'] = pd.to_datetime(str(start)[:11])
    print(start+": Done", end=' | ')
    return df

def get_clicks(all_links,keen='keen_creds'):
    """ Get new click data and return dataframe"""
    min_clicks = min(all_links['date'])
    max_clicks = max(all_links['date'])
    yesterday = datetime.datetime.now()
    yesterday = yesterday - datetime.timedelta(1)
    yesterday = datetime.datetime.strftime(yesterday, '%Y-%m-%d')

    if yesterday != datetime.datetime.strftime(max_clicks, '%Y-%m-%d'):
        new_start = datetime.datetime.strftime(max_clicks + datetime.timedelta(days=1),'%Y-%m-%d')
        new_end = yesterday
        print("Start: " + new_start)
        print("End: " + new_end)
        hour_interval = 24

        timeframe = eds.timeframe_gen(new_start, new_end, hour_interval=24, tz='US/Eastern')
        pool_iter = [(keen, i[0], i[1]) for i in timeframe]

        pool = Pool(8)
        data = pool.starmap(link_clicks, pool_iter)
        print("Clicks: Done")
        pool.close()
        pool.join()

        dfe = pd.concat(data)
        dfe = dfe.reset_index(drop=True)

        def easy_date(hour_date):
            """
            Removes hours from datetime
            """
            return(hour_date.date())

        dfe['keen.timestamp'] = pd.to_datetime(dfe['keen.timestamp'])
        dfe['date'] = dfe['keen.timestamp'].apply(easy_date)
        email_clicks = dfe.groupby(['marketing_campaign_info.id','url','url_offset.index'],as_index=False).agg({'result':sum,
                                                                               'start': 'min'})

        os.chdir('/Users/jbuckley/Python Jupyter/Product')
        email_data = pd.read_excel('all_email_overall_data.xlsx')
        df_category = email_data[['marketing_campaign_info.id','marketing_campaign_info.name','email','region','email_cat','uniques']]
        df_category = df_category[df_category['email_cat']!='category-test'].copy()

        new_links = pd.merge(email_clicks,df_category,how='left',on='marketing_campaign_info.id')
        new_links =  new_links.rename(columns={'start':'date'})
        new_links = new_links.reset_index().drop("index",1)
        new_links['CTR'] = new_links['result'] / new_links['uniques']
        new_links['email_cat'] = new_links['email_cat'].str.replace('–\xa0',"- ")
        df_clicks = pd.concat([all_links,new_links])
        df_clicks.to_excel('Email_Clicks.xls')
        return(df_clicks)
    else:
        print("Data all good through " + datetime.datetime.strftime(max_clicks, '%Y-%m-%d'))
        return(all_links)



## Active User functions
def timeframe_gen_interval(start, end, interval='monthly', tz='US/Eastern'):
    """creates timeframe for use in making Keen API calls
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
    if interval == 'monthly':
        hour_interval = 720
    elif interval == 'weekly':
        hour_interval = 168
    elif interval == 'daily':
        hour_interval = 24
    else:
        hour_interval = 24

    end = pd.to_datetime(end)
    end = end + datetime.timedelta(1)
    start_dates = pd.date_range(start, end, freq='D', tz=tz)
    start_dates = start_dates.tz_convert('UTC')
    end_dates = start_dates - pd.Timedelta(hour_interval, unit='h')


    strftime = datetime.datetime.strftime
    time_format = '%Y-%m-%dT%H:%M:%S.000Z'

    start_times = [strftime(i, time_format) for i in start_dates]
    end_times = [strftime(i, time_format) for i in end_dates]

    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]

    return timeframe[:-1]


def actives(keen, email, id_list, date, other_date):
    """
    used to find the article_tags in articles
    """

    event = 'email_open'

    target_property = 'email'

    timeframe = {'start':other_date, 'end':date}
    interval = None
    timezone = None

    property_name1 = 'marketing_campaign_info.id'
    operator1 = 'in'
    property_value1 = id_list

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1}]

    data = keen.count_unique(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    #group_by=group_by,
                    filters=filters)

    df = pd.DataFrame()
    df['result'] = [data]
    df['date'] = [pd.to_datetime(str(date)[:11])]
    df['email'] = [email]
    print(date+": Done",' | ')
    return df

def emails_delivered(keen, email, id_list, date, other_date):
    """
    used to find the article_tags in articles
    """

    event = 'email_delivered'

    target_property = 'email'

    timeframe = {'start':other_date, 'end':date}
    interval = None
    timezone = None

    property_name2 = 'marketing_campaign_info.id'
    operator2 = 'in'
    property_value2 = id_list

    filters = [{"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    data = keen.count_unique(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    #group_by=group_by,
                    filters=filters)

    df = pd.DataFrame()
    df['result'] = [data]
    df['date'] = [pd.to_datetime(str(date)[:11])]
    df['email'] = [email]
    print(date+": Done",' | ')
    return df

def emails_processed(keen, email, id_list, date, other_date):
    """
    used to find the article_tags in articles
    """

    event = 'email_processed'

    target_property = 'email'

    timeframe = {'start':other_date, 'end':date}
    interval = None
    timezone = None

    #group_by = ('category')

    property_name2 = 'marketing_campaign_info.id'
    operator2 = 'in'
    property_value2 = id_list

    filters = [{"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    data = keen.count_unique(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    filters=filters)

    df = pd.DataFrame()
    df['result'] = [data]
    df['date'] = [pd.to_datetime(str(date)[:11])]
    df['email'] = [email]
    print(date+": Done",' | ')
    return df

def active_email_user_data(start, end, email, id_list, interval='monthly',function=actives):
    """
    new_start = '2018-01-01'
    new_end = '2018-05-08'
    interval = 'monthly', 'weekly', 'daily'
    function = actives, emails_delivered, emails_processed

    """

    timeframe = timeframe_gen_interval(start, end, interval=interval, tz='US/Eastern')
    pool_iter = [(keen, email, id_list ,i[0], i[1]) for i in timeframe]

    pool = Pool(8)
    data = pool.starmap(function, pool_iter)
    pool.close()
    pool.join()

    dfe = pd.concat(data)
    dfe = dfe.reset_index(drop=True)
    dfe = dfe.rename(columns={'result':(interval+"_"+function.__name__)})
    return dfe

def group_overall_emails(email_data,exclude_list = []):
    """
    - Takes DataFrame for metadata and a list of campaign IDs to exclude
    - Returns Dictionary grouped by email and region

    2803157 ==> April 18th when all DB emails were sent through the UK distribution
    """
    exclude_list = exclude_list

    email_edition_dict={}
    emails =list(set(email_data['email']))
    clean_emails = [x for x in emails if str(x) != 'nan']
    for email in clean_emails:
        dft = email_data[(email_data['email']==email)&(email_data['date']>'2017-09-15')].copy()
        campaign_ids=[]
        campaign_ids = list(set(dft['marketing_campaign_info.id']))
        new_list=[]
        for i in campaign_ids:
            try:
                if int(i) not in exclude_list:
                    new_list.append(int(i))
                else:
                    pass
            except:
                pass
        new_list = [str(i) for i in new_list]
        email_edition_dict.setdefault((email),[]).append(new_list)
    return(email_edition_dict)

def group_reg_emails(email_data,exclude_list = [2803157]):
    """
    - Takes DataFrame for metadata and a list of campaign IDs to exclude
    - Returns Dictionary grouped by email and region

    2803157 ==> April 18th when all DB emails were sent through the UK distribution
    """
    exclude_list = exclude_list

    email_edition_dict={}
    emails =list(set(email_data['email']))
    clean_emails = [x for x in emails if str(x) != 'nan']
    for email in clean_emails:
        dft = email_data[(email_data['email']==email)&(email_data['date']>'2017-09-15')].copy()
        for region in set(dft['region']):
            campaign_ids=[]
            dfx = dft[(dft['region']==region)].copy()
            campaign_ids = list(set(dfx['marketing_campaign_info.id']))
            new_list=[]
            for i in campaign_ids:
                try:
                    if int(i) not in exclude_list:
                        new_list.append(int(i))
                    else:
                        pass
                except:
                    pass
            new_list = [str(i) for i in new_list]
            email_edition_dict.setdefault((email+": "+region),[]).append(new_list)
    return(email_edition_dict)

def get_Overall_email_actives(df_overall,global_ids,db_min=330000,obsession_min=140000,quartzy_min=45000,directory='/Users/jbuckley/Python Jupyter/Product',xlsx_name='Overall_subscriber_actives.xlsx'):
    """ Active user data for Daily Brief, Obsession, Quartzy

    Takes previous DataFrame with active user numbers for emails with sub-regions

    db_min = cutoff line where subscribers should not be below
    obsession_min = cutoff line where subscribers should not be below
    quartzy_min = cutoff line where subscribers should not be below
    """
    df_overall = df_overall[['daily_actives', 'date', 'email', 'weekly_actives', 'monthly_actives',
       'daily_emails_delivered', 'daily_emails_processed']].copy()

    min_actives = min(df_overall['date'])
    max_actives = max(df_overall['date'])
    yesterday = datetime.datetime.now()
    yesterday = yesterday - datetime.timedelta(1)
    yesterday = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    print("Last update: "+ str(max_actives)[:11])

    if yesterday != datetime.datetime.strftime(max_actives, '%Y-%m-%d'):
        new_start = datetime.datetime.strftime(max_actives + datetime.timedelta(days=1),'%Y-%m-%d')
        new_end = yesterday
        print("Start: " + new_start)
        print("End: " + new_end)
        hour_interval = 24

        new_dict={}
        for k,v in global_ids.items():
            #print(k,v)
            v = v[0]
            df_MAU = active_email_user_data(new_start, new_end, k, v, interval='monthly',function=actives)
            df_WAU = active_email_user_data(new_start, new_end,k,v, interval='weekly',function=actives)
            df_DAU = active_email_user_data(new_start, new_end,k,v, interval='daily',function=actives)
            df_delivered = active_email_user_data(new_start, new_end,k,v, interval='daily',function=emails_delivered)
            df_processed = active_email_user_data(new_start, new_end,k,v, interval='daily',function=emails_processed)

            df_all = pd.merge(df_DAU,df_WAU,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_MAU,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_delivered,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_processed,on=['date','email'],how='inner')

            new_dict[k] = df_all
        new_df = pd.concat(new_dict.values())
        # Combine new data with old data
        overall_actives = pd.concat([df_overall,new_df])
        overall_actives = overall_actives.sort_values('date')
        overall_actives = overall_actives.reset_index().drop('index',1)

    else:
        # Take old data set because already fully updated
        overall_actives = df_overall.copy()
        overall_actives = overall_actives.sort_values('date')
        overall_actives = overall_actives.reset_index().drop('index',1)
        print("All data updated through yesterday!")
        print("")

    # Clean data, interpolate subscribers
    regional_emails = ['Quartzy','Obsession', 'Daily Brief']
    other_dict={}
    for reg in regional_emails:
        dft = []
        dft = overall_actives[overall_actives['email']==reg].copy()
        my_list = dft['daily_emails_processed'].replace(0,np.nan)
        new_list=[]
        if reg == 'Daily Brief':
            for (i, item) in enumerate(my_list):
                if item < db_min:
                    new_list.append(np.nan)
                elif item >= db_min:
                    new_list.append(item)
                else:
                    new_list.append(np.nan)
        elif reg == 'Quartzy':
            for (i, item) in enumerate(my_list):
                if item < quartzy_min:
                    new_list.append(np.nan)
                elif item >= quartzy_min:
                    new_list.append(item)
                else:
                    new_list.append(np.nan)
        elif reg == 'Obsession':
            for (i, item) in enumerate(my_list):
                if item < obsession_min:
                    new_list.append(np.nan)
                elif item >= obsession_min:
                    new_list.append(item)
                else:
                    new_list.append(np.nan)
        else:
            pass
        dft['processed_int'] = new_list
        dft['processed_int'] = dft['processed_int'].interpolate()
        other_dict[reg] = dft
        print(reg,"avg. subscribers:", str(int(dft['processed_int'].mean())))

    # Clean and add columns
    df_final = pd.concat(other_dict.values())
    df_final['date_adjusted'] = df_final['date'] - datetime.timedelta(1)
    # New columns for edition detail
    df_final['edition'] = df_final['email']
    df_final['region'] = 'Global'
    df_final = df_final[['daily_actives', 'date', 'email', 'weekly_actives', 'monthly_actives',
           'daily_emails_delivered', 'daily_emails_processed', 'date_adjusted',
           'processed_int', 'edition', 'region']]
    df_final = df_final.replace(0,np.nan)

    # Overwrite file in directory
    os.chdir(directory)
    df_final.to_excel(xlsx_name)
    return(df_final)


def get_Region_email_actives(df_actives,email_edition_dict,directory='Users/jbuckley/Python Jupyter/Product',xlsx_name='EMAIL_actives_regions.xlsx'):
    """ Active user data for all emails by each edition

    Takes previous DataFrame with active user numbers for every email list

    db_min = cutoff line where subscribers should not be below
    obsession_min = cutoff line where subscribers should not be below
    quartzy_min = cutoff line where subscribers should not be below"""
    min_actives = min(df_actives['date'])
    max_actives = max(df_actives['date'])
    yesterday = datetime.datetime.now()
    yesterday = yesterday - datetime.timedelta(1)
    yesterday = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    print("Last update: " + str(max_actives)[:11])

    ## Update regional Active User data
    if yesterday != datetime.datetime.strftime(max_actives, '%Y-%m-%d'):
        new_start = datetime.datetime.strftime(max_actives + datetime.timedelta(days=1),'%Y-%m-%d')
        new_end = yesterday
        print("Start: " + new_start)
        print("End: " + new_end)
        hour_interval = 24

        new_dict={}
        for k,v in email_edition_dict.items():
            #print(k,v)
            v = v[0]
            df_MAU = active_email_user_data(new_start, new_end, k, v, interval='monthly',function=actives)
            df_WAU = active_email_user_data(new_start, new_end,k,v, interval='weekly',function=actives)
            df_DAU = active_email_user_data(new_start, new_end,k,v, interval='daily',function=actives)
            df_delivered = active_email_user_data(new_start, new_end,k,v, interval='daily',function=emails_delivered)
            df_processed = active_email_user_data(new_start, new_end,k,v, interval='daily',function=emails_processed)

            df_all = pd.merge(df_DAU,df_WAU,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_MAU,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_delivered,on=['date','email'],how='inner')
            df_all = pd.merge(df_all,df_processed,on=['date','email'],how='inner')

            new_dict[k] = df_all

        new_df = pd.concat(new_dict.values())

        small_dict={}
        for version in set(new_df['email']):
            dft = new_df[new_df['email']==version].copy()
            dft['processed_int'] = dft['daily_emails_processed'].replace(0,np.nan)
            small_dict[version] = dft

        new_df = pd.concat(small_dict.values())
        new_df['date_adjusted'] = new_df['date'] - datetime.timedelta(1)

        # New columns for edition detail
        def email_split(category):
            return(category.split(": ")[0])
        def region_split(category):
            return(category.split(": ")[1])

        new_df['edition'] = new_df['email'].apply(email_split)
        new_df['region'] = new_df['email'].apply(region_split)
        new_df = new_df[df_actives.columns]

        # Combine with original DF
        new_actives = pd.concat([df_actives,new_df])

        other_dict={}
        for version in set(new_actives['email']):
            dft = new_actives[new_actives['email']==version].copy()
            dft['processed_int'] = dft['processed_int'].interpolate()
            other_dict[version] = dft

        new_actives = pd.concat(other_dict.values())
        new_actives = new_actives.replace(to_replace=0,value="")
        # Write to directory
        os.chdir(directory)
        new_actives.to_excel(xlsx_name)
        return(new_actives)
    else:
        return(df_actives)
