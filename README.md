# Airbnb-Listing-and-Pricing
airbnb pricing intelligence and demand forcasting system using snowflake advanced sql(cte,merge,window function,task,automation)
create or replace database airbnb_db;
use database airbnb_db;

create schema airbnb_db.analytics;
use airbnb_db.analytics;

create or replace warehouse airbnb_wh
with warehouse_size ='xsmall'
auto_suspend = 60
auto_resume = true;
use warehouse airbnb_wh;

create or replace stage airbnb_stage;
list @airbnb_stage;

create or replace table airbnb_raw(
listing_id string,
date date,
price number,
availability string,
min_nights number,
neighbourhood string
);

merge into airbnb_raw t
using staging_table s
on t.listing_id = s.listing_id
and t.date = s.date

when matched then
update set
t.price = s.price,
t.availability = s.availability

when not matched then
insert(listing_id,date,price,availability,min_nights,neighbourhood)
values(s.listing_id,s.date,s.price,s.availability,s.min_nights,s.neighbourhood);

with listing_stats as(
select
listing_id,
count(*) as total_bookings,
avg(price) as avg_price
from airbnb_raw
group by listing_id
)
select*
from listing_stats
where total_bookings > 1;

with ranked_listings as(
select
listing_id,
neighbourhood,
price,
rank() over (partition by neighbourhood order by price desc) as price_rank
from airbnb_raw
)
select* from ranked_listings;


select listing_id,
date,
price,
lag(price) over (partition by listing_id order by date) as previous_price,
price - lag(price) over (partition by listing_id order by date) as price_change
from airbnb_raw;

select
listing_id,
date,
price,
lead(price) over (partition by listing_id order by date) as next_price
from airbnb_raw;

create or replace task airbnb_db.analytics.daily_airbnb_refresh
warehouse = airbnb_wh
schedule = 'USING CRON 0 2 * * * UTC'
as
merge into airbnb_raw t
using staging_table s
on t.listing_id = s.listing_id
and t.date = s.date
when matched then
update set
  t.price = s.price,
  t.availability = s.availability
when not matched then
insert(listing_id,date,price,availability,min_nights,neighbourhood)
values(s.listing_id,s.date,s.price,s.availability,s.min_nights,s.neighbourhood);


create or replace table airbnb_insights as
select
neighbourhood,
avg(price) as avg_price,
count(*) as total_listings
from airbnb_raw
group by neighbourhood;

select* from airbnb_insights;
