-- Code to fetch engaged viewers from Intro the Posh Shows Meet and Greet

--  Python code to temporary view the dw_users_cs table in Databricks from Redshift

from pyspark.sql import *
from pyspark import *
from pyspark.sql import DataFrame
sc = spark._sc
helpers = sc._jvm.com.poshmark.spark.helpers
Config = helpers.Config
Redshift = helpers.Redshift
Config.reloadConfigs(sc._jsc.sc())
Config.registerUDFs()
query = "select * from analytics.dw_users_cs"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("dw_users_cs")
query="select * from analytics_scratch.l365d_seller_segment_gmv"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("l365d_seller_segment_gmv")
query="select * from analytics_scratch.l365d_shopper_segment"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("l365d_shopper_segment")



-- # Python code to temporary view the dw_users_info table in Databricks from Redshift

from pyspark.sql import *
from pyspark import *
from pyspark.sql import DataFrame
sc = spark._sc
helpers = sc._jvm.com.poshmark.spark.helpers
Config = helpers.Config
Redshift = helpers.Redshift
Config.reloadConfigs(sc._jsc.sc())
Config.registerUDFs()
query = "select * from analytics.dw_users_info"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("dw_users_info")
query="select * from analytics_scratch.l365d_seller_segment_gmv"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("l365d_seller_segment_gmv")
query="select * from analytics_scratch.l365d_shopper_segment"
DataFrame(Redshift.getQueryDF(query), spark).createOrReplaceTempView("l365d_shopper_segment")






-- Query to fetch viewers who has joined the show along with their host status and total show watch time
-- Host status of the users are categoried into 5 types 'no_waitlist','waitlist','host eligible', 'host access received', 'schedule activated host' and ' host activated'
-- total show watch time is categorised into 2 based on the show watch time duration as '>= 3 minutes' and '<3 minutes'

SELECT viewer_id,
       platform_user_id,
       live_show_host_activated_at::DATE AS live_show_host_activated_date,
       silent_show_host_activated_at::DATE AS silent_show_host_activated_date,
       show_host_activated_at::DATE AS show_host_activated_date,
       show_scheduled_activated_at::DATE AS show_scheduled_activated_date,
       show_host_access_at::DATE AS show_host_access_date,
       show_host_eligible_at::DATE AS show_host_eligible_date,
       in_show_host_certification_queue_at::DATE AS in_show_host_certification_queue_date,
       CASE
           WHEN show_host_activated_at IS NOT NULL THEN 'host_activated'
           WHEN show_scheduled_activated_at IS NOT NULL THEN 'schedule_activated'
           WHEN show_host_access_at IS NOT NULL THEN 'host_access_received'
           WHEN show_host_eligible_at IS NOT NULL THEN 'host_eligible'
           WHEN in_show_host_certification_queue_at IS NOT NULL THEN 'waitlist'
           ELSE 'no_waitlist'
       END AS Host_status,
       CASE
           WHEN total_watched_show_minutes >= 3 THEN '>= 3 minutes'
           ELSE '< 3 minutes'
       END AS Show_Watched_Minutes
FROM s3_analytics_journal.dw_show_viewer_events_cs AS view
LEFT JOIN s3_analytics.dw_shows AS shows ON shows.show_id = view.show_id
LEFT JOIN dw_users_cs AS user_cs ON user_cs.user_id = view.viewer_id
LEFT JOIN s3_analytics.dw_users AS user ON user.user_id = view.viewer_id
LEFT JOIN dw_users_info AS user_info ON user_info.user_id = view.viewer_id
WHERE shows.show_id='65e0d8a467755d050b9d37b2';

--------------------------------------------------------------------------------

-- Code to get check RSVPed rate

-- Creating a temporary view table named rsvp_base
-- Selecting the user_id from the actor.id column in the raw_events table,
-- where the event_date is between '2024-02-29' and '2024-03-14',
-- the verb is 'bookmark',
-- the show_type of the direct_object is 'live',
-- and the id of the show is '65e0d8a467755d050b9d37b2'



CREATE OR REPLACE TEMPORARY VIEW rsvp_base AS
SELECT re.actor.id AS user_id
FROM raw_events AS re
WHERE 1 = 1
  AND re.event_date >= '2024-02-29'
  AND re.event_date <= '2024-03-14'
  AND re.verb = 'bookmark'
  AND re.direct_object.show_type = 'live'
  AND re.direct_object.id = '65e0d8a467755d050b9d37b2';




-- Selecting distinct user_id, viewer_id, and platform_user_id 
-- Query to fetch the show viewers who have saved the show
-- Left Joined the views who viewed the show on 'viewer_id' from 'dw_show_viewer_events_cs' table to the 'user_id' from 'rsvp_base' 
-- Left joined the 'platform_id' from 'user_info' table to 'rsvp_base' table to get platform id of all users

SELECT DISTINCT rsvp_base.user_id, 
                view.viewer_id, 
                user_info.platform_user_id
FROM rsvp_base
LEFT JOIN (
  SELECT DISTINCT viewer_id
  FROM s3_analytics_journal.dw_show_viewer_events_cs
  WHERE show_id = '65e0d8a467755d050b9d37b2'
) AS view ON rsvp_base.user_id = view.viewer_id
LEFT JOIN dw_users_info AS user_info ON user_info.user_id = rsvp_base.user_id;
