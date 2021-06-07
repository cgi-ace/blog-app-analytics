-- These BigQuery SQL clauses are examples from CGI Advanced Analytics blog post https://www.cgi.com/fi/fi/blogi/???
-- Public Demo Dataset: 
-- https://console.cloud.google.com/bigquery?p=firebase-public-project&d=analytics_153293282&t=events_20180915&page=table

---with Pseudo column _TABLE_SUFFIX
 
select Count(distinct user_pseudo_id) AUD, PARSE_DATE('%Y%m%d', event_date) as event_date,geo.country,device.operating_system
from `firebase-public-project.analytics_153293282.events_*` 
where event_name = 'user_engagement'
and _TABLE_SUFFIX = '20180821'
group by event_date,geo.country,device.operating_system


---- First day and last day of a month for dynamically using it in _TABLE_SUFFIX 
with tablemonth as (
SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_TRUNC(CURRENT_DATE(), MONTH)) as string) as st_date_month, 
CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)) as string) as end_date_month 
)
select * from tablemonth

---- UNNEST Function example for Quick_play users

SELECT count(distinct user_pseudo_id) user_count  , 
(SELECT value.string_value FROM UNNEST(user_properties) where key= 'plays_quickplay') as quick_play_flag ,
PARSE_DATE('%Y%m%d', event_date) as event_date,
geo.country,
device.operating_system
from `firebase-public-project.analytics_153293282.events_*` 
where  _TABLE_SUFFIX ='20180821'
group by event_date,quick_play_flag,geo.country,device.operating_system
order by geo.country,device.operating_system

-------------------------
-- Avg Session Interval Across Daysuseful for Apps like Food Delivery Apps.
-- Step 1 is to find the minimum of event time for engament event for ever user; as a sample lets try for Australia. Although there can be many engagement event for a user
-- in a day, we are intrested in only earliest engagement event

SELECT
user_pseudo_id, 
event_date,
min(TIMESTAMP_MICROS(event_timestamp)) as session_start,
geo.country as country,
device.operating_system as OS  
FROM  `firebase-public-project.analytics_153293282.events_*`  
where  event_name = 'user_engagement'
and  _TABLE_SUFFIX between '20180601'and '20181031'
and geo.country = 'Australia'
group by user_pseudo_id,event_date,country,OS

-- Step 2 is to identify the previous engagement session for every user

select 
stepone.user_pseudo_id,
stepone.event_date,
stepone.session_start,
stepone.country,
stepone.OS,
lag(stepone.session_start,1) over (partition by stepone.user_pseudo_id order by stepone.session_start) as last_session_start_ts
from
	(SELECT
		user_pseudo_id, 
		event_date,
		min(TIMESTAMP_MICROS(event_timestamp)) as session_start,
		geo.country as country,
		device.operating_system as OS  
		FROM  `firebase-public-project.analytics_153293282.events_*`  
		where  event_name = 'user_engagement'
		and  _TABLE_SUFFIX between '20180601'and '20181031'
		and geo.country = 'Australia'
		group by user_pseudo_id,event_date,country,OS
		
	) stepone
group by 
stepone.user_pseudo_id,
stepone.event_date,
stepone.session_start,
stepone.country,
stepone.OS
order by user_pseudo_id

-- Step 3 is to find the session invertval for every user for all days in the period

select 
    steptwo.user_pseudo_id,
    steptwo.event_date,
    DATE_DIFF(date(steptwo.session_start), COALESCE(date(steptwo.last_session_start_ts),date(steptwo.session_start)), DAY) AS session_invertal,
    steptwo.country,
    steptwo.OS
    from (
        select 
        stepone.user_pseudo_id,
        stepone.event_date,
        stepone.session_start,
        stepone.country,
        stepone.OS,
        lag(stepone.session_start,1) over (partition by stepone.user_pseudo_id order by stepone.session_start) as last_session_start_ts
        from
            (SELECT
                user_pseudo_id, 
                event_date,
                min(TIMESTAMP_MICROS(event_timestamp)) as session_start,
                geo.country as country,
                device.operating_system as OS  
                FROM  `firebase-public-project.analytics_153293282.events_*`  
                where  event_name = 'user_engagement'
                and  _TABLE_SUFFIX between '20180601'and '20181031'
                and geo.country = 'Australia'
                group by user_pseudo_id,event_date,country,OS
                
            ) stepone
            group by 
            stepone.user_pseudo_id,
            stepone.event_date,
            stepone.session_start,
            stepone.country,
            stepone.OS
            order by user_pseudo_id
            
    ) steptwo 

---
-- Step 4 is to find the total unique users for a given day and Sum of session interval from step 3. Now the sum_session_interval/user_count will give you the avg session interval
-- for a given day. You can then pick the days you want for reporting period

select 
count(distinct Stepthree.user_pseudo_id) as user_count,
sum(Stepthree.session_invertal) as sum_session_invertal_in_days,
Stepthree.event_date,
Stepthree.country,
Stepthree.OS
from
(
    select 
    steptwo.user_pseudo_id,
    steptwo.event_date,
    DATE_DIFF(date(steptwo.session_start), COALESCE(date(steptwo.last_session_start_ts),date(steptwo.session_start)), DAY) AS session_invertal,
    steptwo.country,
    steptwo.OS
    from (
        select 
        stepone.user_pseudo_id,
        stepone.event_date,
        stepone.session_start,
        stepone.country,
        stepone.OS,
        lag(stepone.session_start,1) over (partition by stepone.user_pseudo_id order by stepone.session_start) as last_session_start_ts
        from
            (SELECT
                user_pseudo_id, 
                event_date,
                min(TIMESTAMP_MICROS(event_timestamp)) as session_start,
                geo.country as country,
                device.operating_system as OS  
                FROM  `firebase-public-project.analytics_153293282.events_*`  
                where  event_name = 'user_engagement'
                and  _TABLE_SUFFIX between '20180601'and '20181031'
                and geo.country = 'Australia'
                group by user_pseudo_id,event_date,country,OS
                
            ) stepone
            group by 
            stepone.user_pseudo_id,
            stepone.event_date,
            stepone.session_start,
            stepone.country,
            stepone.OS
            order by user_pseudo_id
            
    ) steptwo 
) Stepthree
where Stepthree.session_invertal >0
group by 
Stepthree.event_date,
Stepthree.country,
Stepthree.OS

--------------- Retention--- A sample

with Week_0 AS (
select distinct user_pseudo_id
from `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'first_open'
AND _TABLE_SUFFIX Between '20180820' AND '20180826' 
),
week_1  AS (
select distinct user_pseudo_id
from `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'user_engagement'
AND _TABLE_SUFFIX Between '20180827' AND '20180902' 
),
week_2 AS (
select distinct user_pseudo_id
from `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'user_engagement'
AND _TABLE_SUFFIX Between '20180903' AND '20180909' 
)
select  
(select count(*)  from Week_0 ) as w0_cohort,
(select count(*) from  week_1 Join Week_0 using (user_pseudo_id)) as w1_cohort ,
(select count(*) from  week_2 Join Week_0 using (user_pseudo_id)) as w2_cohort
