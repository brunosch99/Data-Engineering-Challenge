#!/usr/bin/env python
# coding: utf-8

import json
import psycopg2
import pandas as pd
import numpy as np
import config as cfg

def get_campaign_id(line):
    """
    Receives a line from a url and gets the campaign_id if exists
    Returns campaign_id, if doesn't exists, return 0
    """
    
    if len(line.split('?')) == 2:
        if len(line.split('?')[1].split('&')) == 1:
            return int(line.split('?')[1].split('=')[1])
        else:
            return int(line.split('?')[1].split('&')[1].split('=')[1])
    #Returns a default value for campaign_id
    return 0

def get_ad_creative_id(line):
    """
    Receives a line from a url and gets the ad_creative_id if exists
    Returns ad_creative_id, if doesn't exists, return 0
    """
    
    if len(line.split('?')) == 2:
        if len(line.split('?')[1].split('&')) == 2:
            return int(line.split('?')[1].split('&')[0].split('=')[1])
    #Returns a default value for ad_creative_id
    return 0

def generate_create_table_script(df, table_name):
    """
    Receives a dataframe and a table name
    Returns a SQL script that creates the table with the received parameter name and with the fields and its datatypes from the dataframe
    """
    
    fields_and_types = []
    for column in df.columns:
        if str(df[column].dtype) == 'int64':
            fields_and_types.append([column,"INT"])
        elif str(df[column].dtype) == 'float64':
            fields_and_types.append([column,"FLOAT"])
        else:
            fields_and_types.append([column,"VARCHAR(100)"])
    script = "CREATE TABLE {} (".format(table_name)
    for f in fields_and_types:
        script += f[0] +" "+ f[1]
        if f != fields_and_types[len(fields_and_types)-1]:
            script+=","
    script+=");"
    return script

def run_query(query, commit=False):
    """
    Receives a query and a commit flag
    Run the query with the global connection variable and commits if the flago equals True
    """
    
    global connection
    if connection:
        cursor = connection.cursor()
        cursor.execute(query)
        if commit:
            connection.commit()

def connect_database(database):
    """
    Receives a database to connect and return its connection
    Uses the configs from the config.py file
    """
    
    try:
        connection = psycopg2.connect(user = cfg.user,
                                      password = cfg.password,
                                      host = cfg.host,
                                      port = cfg.port,
                                      database = database)
        print("Connection Success!")
        return connection
    except:
        print("Failed connection!")
        return None

def generate_insert_script(dictionary, table):
    """
    Receives a dictionary and a table 
    Returns a SQL script that inserts on that table the information from the dictionary received
    """
    
    script = "INSERT INTO {} VALUES(".format(table)
    for k in dictionary:
        if type(dictionary[k]) == int or type(dictionary[k]) == float:
            script+=str(dictionary[k])+","
        else:
            script+="'"+str(dictionary[k])+"'"+","
    script+=")"
    script = script.replace(",)",");")
    return script

def load_dataframe_into_table(df, table):
    """
    Receives a list of df and a table
    Inserts all df lines into the parameter table
    """
    
    #For a better performance the dataframe is converted into a dictionary
    dictionary = df.to_dict('records')
    for d in dictionary:
        if d == dictionary[len(dictionary)-1]:
            run_query(generate_insert_script(d, table), True)
        else:
            run_query(generate_insert_script(d, table))

def insert_dfs_into_database(df_table):
    for dt in df_table:
        print("Creating {} table".format(dt[1]))
        run_query(generate_create_table_script(dt[0], dt[1]), True)
        print("Inserting into {}".format(dt[1]))
        load_dataframe_into_table(dt[0], dt[1])


#Defining files paths
google_ad_path = r'C:\Users\BlueShift\Documents\Data-Engineering-Challenge\datasets\google_ads_media_costs.jsonl'
facebook_ad_path = r'C:\Users\BlueShift\Documents\Data-Engineering-Challenge\datasets\facebook_ads_media_costs.jsonl'
pageview_path = r'C:\Users\BlueShift\Documents\Data-Engineering-Challenge\datasets\pageview.txt'
customer_leads_funnel_path = r'C:\Users\BlueShift\Documents\Data-Engineering-Challenge\datasets\customer_leads_funnel.csv'

#Matrix that will contain a df and a name for its table in the database
df_table = []

#Generating google_ad_df from google_ads_media_costs.jsonl 
google_ad_df = pd.read_json(google_ad_path, lines=True)

#Generating facebook_ad_df from facebook_ads_media_costs.jsonl 
facebook_ad_df = pd.read_json(facebook_ad_path, lines=True)

#Generating pageview_df from pageview.txt
#Some columns were removed and some were created using other columns
pageview_df = pd.read_csv(pageview_path, delimiter=' ', header=None)
pageview_df.drop([1,4,5,7,8,10,11], axis=1, inplace=True)
pageview_df.columns = ['ip', 'date', 'hour', 'url', 'device_id', 'referer']
pageview_df['datetime'] = pageview_df['date'].astype('str').apply(lambda line: line.replace("[", "")) + " " + pageview_df['hour'].astype('str').apply(lambda line: line.replace("]", ""))
pageview_df['campaign_id'] = pageview_df['url'].apply(get_campaign_id).astype('int64')
pageview_df['ad_creative_id'] = pageview_df['url'].apply(get_ad_creative_id)
pageview_df.drop(['date', 'hour'], axis=1, inplace=True)

#Generating customer_leads_funnel_df from customer_leads_funnel.csv
customer_leads_funnel_df = pd.read_csv(customer_leads_funnel_path, header=None)
customer_leads_funnel_df.columns = ['device_id', 'lead_id', 'registered_at', 'credit_decision', 'credit_decision_at', 'signed_at', 'revenue']
customer_leads_funnel_df['signed_at'].fillna('-', inplace = True)
customer_leads_funnel_df['revenue'].fillna(0, inplace = True)

df_table.append([google_ad_df,"google_ads_media_costs"])
df_table.append([facebook_ad_df,"facebook_ads_media_costs"])
df_table.append([pageview_df,"pageview"])
df_table.append([customer_leads_funnel_df,"customer_leads_funnel"])

#Connects to database
connection = connect_database("marketing_campaign")

if connection is not None:
    insert_dfs_into_database(df_table)
    print("Ingestion Finished")
    
    create_campaign_stats_query = """
        CREATE TABLE campaign_stats as(
        SELECT c.*, l.device_id, l.lead_id, l.credit_decision, l.revenue
        FROM
            (SELECT G.google_campaign_id as campaign_id,
                    G.google_campaign_name as campaign_name,
                    G.ad_creative_id as ad_creative_id,
                    G.ad_creative_name as ad_creative_name,
                    SUM(G.clicks) as clicks,
                    SUM(G.impressions) as impressions,
                    SUM(G.cost) as cost
            FROM google_ads_media_costs G
            GROUP BY G.google_campaign_id,
                G.google_campaign_name,
                G.ad_creative_id,
                G.ad_creative_name
            UNION ALL
            SELECT F.facebook_campaign_id AS campaign_id,
                    F.facebook_campaign_name AS campaign_name,
                    0 as ad_creative_id,
                    null as ad_creative_name,
                    SUM(F.clicks) AS clicks,
                    SUM(F.impressions) AS impressions,
                    SUM(F.cost) AS cost
            FROM facebook_ads_media_costs F
            GROUP BY F.facebook_campaign_id,
                F.facebook_campaign_name) C
        INNER JOIN pageview P
        ON P.campaign_id  = C.campaign_id AND  P.ad_creative_id  = C.ad_creative_id
        INNER JOIN customer_leads_funnel L
        ON L.device_id = p.device_id);
        """
    run_query(create_campaign_stats_query, True)

    connection.close()
else:
    print("No database connection!")