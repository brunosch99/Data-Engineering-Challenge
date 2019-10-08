# Data-Engineering-Challenge

## SQL Create Table Script - Answer Table
``` sql
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
            F.facebook_campaign_name
        ) C
    INNER JOIN pageview P
    ON P.campaign_id  = C.campaign_id AND  P.ad_creative_id  = C.ad_creative_id
    INNER JOIN customer_leads_funnel L
    ON L.device_id = p.device_id
);
```

## SQL queries answers

  - What was the most expensive campaign?
  ``` sql
  SELECT DISTINCT campaign_id, campaign_name, ad_creative_id, ad_creative_name, clicks, impressions, cost
  FROM campaign_stats
  WHERE cost = (
        SELECT MAX(cost) 
        FROM campaign_stats
  );
  ```
  - What was the most profitable campaign?
  ``` sql
  SELECT R.*, R.total_revenue - R.cost AS profit 
  FROM (
    SELECT campaign_id, campaign_name, ad_creative_id, ad_creative_name, cost, SUM(revenue) as total_revenue
    FROM campaign_stats
    GROUP BY campaign_id, campaign_name, ad_creative_id, ad_creative_name, cost
  ) as R
  ORDER BY profit desc
  LIMIT 1;
  ```
  - Which ad creative is the most effective in terms of clicks?
  ``` sql
  SELECT DISTINCT ad_creative_id, ad_creative_name, clicks, impressions, cost
  FROM campaign_stats
  WHERE clicks =(
    SELECT MAX(clicks)
    FROM campaign_stats
    WHERE ad_creative_id != 0
   );
  ```
  - Which ad creative is the most effective in terms of generating leads?
  ``` sql
  SELECT campaign_id, campaign_name, ad_creative_id, ad_creative_name, COUNT(DISTINCT lead_id) as total_leads
  FROM campaign_stats
  WHERE ad_creative_id != 0
  GROUP BY campaign_id, campaign_name, ad_creative_id, ad_creative_name
  ORDER BY total_leads DESC
  LIMIT 1;
  ```
  
## How to reproduce the solution

### Installing Postgres
  - In my solution I used postgres with docker-machine, the image I chose was the 12.0-alpine, because it is a lighter version but any postgres 12 version works
  - Starting the container, you change the parameters name, POSTGRES_PASSWORD and -p
  ``` sh
  docker run --name some-postgres -e POSTGRES_PASSWORD=somepass -p 5432:5432 -d postgres-12.0-alpine
  ```
  - Test connection to the container with any database client you like
  - Create a new database for the new tables

### Running Ingestion Script
  - Change the config.py file with the configurations from your postgres server and with the database name you created
  - Change the paths variables to the paths of the files on your computer
  ``` python 
  google_ad_path = ''
  facebook_ad_path = ''
  pageview_path = ''
  customer_leads_funnel_path = ''
  ```
  - Run the script marketing_ingestion.py
  
### Get answers
  - With any database client, run the answer queries that are listed at the beginning of document
