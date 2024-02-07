CREATE Table Jaswanthv.daily_web_metrics (
user_id BIGINT,
metric_name VARCHAR,
metric_value BIGINT,
date DATE
)
WITH (
format = 'PARQUET',
partitioning = ARRAY['metric_name', 'date']
);

Insert into Jaswanthv.daily_web_metrics
Select
user_id,
'visited_home_page' as metric_name,
COUNT(CASE WHEN url = '/' Then 1 End) As metric_value,
CAST(event_time As DATE) as date
from bootcamp.web_events
GROUP BY user_id, CAST(event_time As DATE);

-- Aggregartions on daily_web_metrics
Select
 date,
 metric_name,
 SUM(metric_value)
From Jaswanthv.daily_web_metrics
Group by date,metric_name
Order BY 3 Desc;

-- Create monthly aggregation table
CREATE TABLE Jaswanthv.monthly_array_web_metrics (
user_id BIGINT,
metric_name VARCHAR,
metric_array ARRAY(INTEGER),
month_start VARCHAR
)
With
(
format = 'PARQUET',
partitioning = ARRAY['metric_name','month_start']
);

-- Insert data into monthly aggr

Insert into Jaswanthv.monthly_array_web_metrics
With yesterday As
(Select * from Jaswanthv.monthly_array_web_metrics
where month_start ='2023-08-01'
),
today As
(Select * from Jaswanthv.daily_web_metrics
Where date = DATE('2023-08-02')
)
SELECT
 COALESCE(t.user_id, y.user_id) As user_id,
 COALESCE(t.metric_name, y.metric_name) As metric_name,
ARRAY[t.metric_value] As metric_array,
 '2023-08-01' As month_start 
From today t FULL OUTER JOIN
yesterday y on t.user_id = y.user_id And t.metric_name = y.metric_name;

Insert into Jaswanthv.monthly_array_web_metrics
With yesterday As
(Select * from Jaswanthv.monthly_array_web_metrics
where month_start ='2023-08-01'
),
today As
(Select * from Jaswanthv.daily_web_metrics
Where date = DATE('2023-08-02')
)
SELECT
 COALESCE(t.user_id, y.user_id) As user_id,
 COALESCE(t.metric_name, y.metric_name) As metric_name,
COALESCE(y.metric_array, REPEAT(null, CAST(DATE_DIFF('day', DATE('2023-08-01'),t.date) AS INTEGER) ))|| ARRAY[t.metric_value] As metric_array,
 '2023-08-01' As month_start 
From today t FULL OUTER JOIN
yesterday y on t.user_id = y.user_id And t.metric_name = y.metric_name;

Delete From Jaswanthv.monthly_array_web_metrics
Where CARDINALITY(metric_array) < 2;

Insert into Jaswanthv.monthly_array_web_metrics
With yesterday As
(Select * from Jaswanthv.monthly_array_web_metrics
where month_start ='2023-08-01'
),
today As
(Select * from Jaswanthv.daily_web_metrics
Where date = DATE('2023-08-03')
)
SELECT
 COALESCE(t.user_id, y.user_id) As user_id,
 COALESCE(t.metric_name, y.metric_name) As metric_name,
COALESCE(y.metric_array, REPEAT(null, CAST(DATE_DIFF('day', DATE('2023-08-01'),t.date) AS INTEGER) ))|| ARRAY[t.metric_value] As metric_array,
 '2023-08-01' As month_start 
From today t FULL OUTER JOIN
yesterday y on t.user_id = y.user_id And t.metric_name = y.metric_name

Delete From Jaswanthv.monthly_array_web_metrics
Where CARDINALITY(metric_array) < 3

Insert into Jaswanthv.monthly_array_web_metrics
With yesterday As
(Select * from Jaswanthv.monthly_array_web_metrics
where month_start ='2023-08-01'
),
today As
(Select * from Jaswanthv.daily_web_metrics
Where date = DATE('2023-08-04')
)
SELECT
 COALESCE(t.user_id, y.user_id) As user_id,
 COALESCE(t.metric_name, y.metric_name) As metric_name,
COALESCE(y.metric_array, REPEAT(null, CAST(DATE_DIFF('day', DATE('2023-08-01'),t.date) AS INTEGER) ))|| ARRAY[t.metric_value] As metric_array,
 '2023-08-01' As month_start 
From today t FULL OUTER JOIN
yesterday y on t.user_id = y.user_id And t.metric_name = y.metric_name

Delete From Jaswanthv.monthly_array_web_metrics
Where CARDINALITY(metric_array) < 4

Insert into Jaswanthv.monthly_array_web_metrics
With yesterday As
(Select * from Jaswanthv.monthly_array_web_metrics
where month_start ='2023-08-01'
),
today As
(Select * from Jaswanthv.daily_web_metrics
Where date = DATE('2023-08-05')
)
SELECT
 COALESCE(t.user_id, y.user_id) As user_id,
 COALESCE(t.metric_name, y.metric_name) As metric_name,
COALESCE(y.metric_array, REPEAT(null, CAST(DATE_DIFF('day', DATE('2023-08-01'),t.date) AS INTEGER) ))|| ARRAY[t.metric_value] As metric_array,
 '2023-08-01' As month_start 
From today t FULL OUTER JOIN
yesterday y on t.user_id = y.user_id And t.metric_name = y.metric_name

Delete From Jaswanthv.monthly_array_web_metrics
Where CARDINALITY(metric_array) < 5

-- Analytical usage of above table

Select 
user_id % 2 As is_user_id_odd, 
metric_name,
SUM(metric_array[1]) As data_for_aug_1,
SUM(metric_array[2]) As data_for_aug_2,
SUM(metric_array[3]) As data_for_aug_3
From Jaswanthv.monthly_array_web_metrics
Group BY 1,2

-- 2nd way 
Select 
user_id % 2 As is_user_id_odd, 
metric_name,
month_start,
ARRAY[
SUM(metric_array[1]),
SUM(metric_array[2]),
SUM(metric_array[3])
]
From Jaswanthv.monthly_array_web_metrics
Group BY 1,2,3

-- 3rd way to unnest index values

with aggregated As
(
Select 
user_id % 2 As is_user_id_odd, 
metric_name,
month_start,
ARRAY[
SUM(metric_array[1]),
SUM(metric_array[2]),
SUM(metric_array[3])
] As agg_array
From Jaswanthv.monthly_array_web_metrics
Group BY 1,2,3
)
Select * from aggregated CROSS JOIN UNNEST (agg_array)
With
ORDINALITY As t(VALUE, INDEX)

with aggregated As
(
Select 
user_id % 2 As is_user_id_odd, 
metric_name,
month_start,
ARRAY[
SUM(metric_array[1]),
SUM(metric_array[2]),
SUM(metric_array[3])
] As agg_array
From Jaswanthv.monthly_array_web_metrics
Group BY 1,2,3
)
Select
is_user_id_odd,
metric_name,
DATE_ADD('day', INDEX - 1,DATE(month_start)) As DATE,
VALUE
 from aggregated CROSS JOIN UNNEST (agg_array)
With
ORDINALITY As t(VALUE, INDEX)

with aggregated As
(
Select 
user_id % 2 As is_user_id_odd, 
metric_name,
month_start,
CASE
 WHEN REDUCE(metric_array, 0, (s,x) -> s + COALESCE(x,0), s -> s) > 0 THEN 'has_data_in_last_3_days'
 ELSE 'empty'
END,
ARRAY[
SUM(metric_array[1]),
SUM(metric_array[2]),
SUM(metric_array[3])
] As agg_array
From Jaswanthv.monthly_array_web_metrics
Group BY 1,2,3,4
)
Select
is_user_id_odd,
metric_name,
DATE_ADD('day', INDEX - 1,DATE(month_start)) As DATE,
VALUE
 from aggregated CROSS JOIN UNNEST (agg_array)
With
ORDINALITY As t(VALUE, INDEX)

SELECT
 user_id,
  metric_name,
  month_start,
  REDUCE(
    metric_array,
    0,
    (s, x) -> s + COALESCE(x, 0),
    s -> s
  ),
  metric_array,
  CASE
    WHEN REDUCE(
      metric_array,
      0,
      (s, x) -> s + COALESCE(x, 0),
      s -> s
    ) > 0 THEN 'has_data_in_last_month'
    ELSE 'empty'
  END
FROM
  Jaswanthv.monthly_array_web_metrics



