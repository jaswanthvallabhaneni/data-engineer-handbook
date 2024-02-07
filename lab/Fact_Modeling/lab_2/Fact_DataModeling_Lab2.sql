CREATE TABLE Jaswanthv.web_users_cumulated
(
user_id BIGINT,
dates_active ARRAY (DATE),
DATE DATE
)
WITH
(FORMAT = 'PARQUET', 
 partitioning = ARRAY['date']
);

INSERT into Jaswanthv.web_users_cumulated
WITH yesterday AS (
Select * From 
Jaswanthv.web_users_cumulated
WHERE DATE = DATE('2022-12-31')
),
today As 
(
	Select 
		user_id,
		CAST(DATE_TRUNC('day', event_time) As Date) As event_date,
		COUNT(1)
	from
	bootcamp.web_events
	WHERE
		DATE_TRUNC('day', event_time) = DATE('2023-01-01')
	GROUP BY
		user_id,
		CAST(DATE_TRUNC('day', event_time) AS DATE)
)
Select 
COALESCE(y.user_id, t.user_id) AS user_id,
CASE
 	WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
 	ELSE ARRAY[t.event_date]
 END AS dates_active,
 DATE('2023-01-01') As Date
from 
yesterday y FULL OUTER JOIN
today t ON y.user_id = t.user_id;

INSERT into Jaswanthv.web_users_cumulated
WITH yesterday AS (
Select * From 
Jaswanthv.web_users_cumulated
WHERE DATE = DATE('2023-01-01')
),
today As 
(
	Select 
		user_id,
		CAST(DATE_TRUNC('day', event_time) As Date) As event_date,
		COUNT(1)
	from
	bootcamp.web_events
	WHERE
		DATE_TRUNC('day', event_time) = DATE('2023-01-02')
	GROUP BY
		user_id,
		CAST(DATE_TRUNC('day', event_time) AS DATE)
)
Select 
COALESCE(y.user_id, t.user_id) AS user_id,
CASE
 	WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
 	ELSE ARRAY[t.event_date]
 END AS dates_active,
 DATE('2023-01-02') As Date
from 
yesterday y FULL OUTER JOIN
today t ON y.user_id = t.user_id

|
|
|

INSERT into Jaswanthv.web_users_cumulated
WITH yesterday AS (
Select * From 
Jaswanthv.web_users_cumulated
WHERE DATE = DATE('2023-01-06')
),
today As 
(
	Select 
		user_id,
		CAST(DATE_TRUNC('day', event_time) As Date) As event_date,
		COUNT(1)
	from
	bootcamp.web_events
	WHERE
		DATE_TRUNC('day', event_time) = DATE('2023-01-07')
	GROUP BY
		user_id,
		CAST(DATE_TRUNC('day', event_time) AS DATE)
)
Select 
COALESCE(y.user_id, t.user_id) AS user_id,
CASE
 	WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
 	ELSE ARRAY[t.event_date]
 END AS dates_active,
 DATE('2023-01-07') As Date
from 
yesterday y FULL OUTER JOIN
today t ON y.user_id = t.user_id;
;


-- Bit conversion of date_array_list
With today As
(
Select * from Jaswanthv.web_users_cumulated
Where Date = DATE('2023-01-07')
),
date_list_int As
(
Select 
user_id,
CAST(SUM(
CASE
  WHEN CONTAINS(dates_active, sequence_date) Then POW(2, 31 - DATE_DIFF('day',sequence_date, date))
  Else 0
  END) AS BIGINT) As history_int
from today
CROSS JOIN UNNEST (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t(sequence_date)
GROUP BY
user_id
)
Select *,
TO_BASE(history_int,2) As history_in_binary
FROM date_list_int

-- Bitwise operations for Analytics

With today As
(
Select * from Jaswanthv.web_users_cumulated
Where Date = DATE('2023-01-07')
),
date_list_int As
(
Select 
user_id,
CAST(SUM(
CASE
  WHEN CONTAINS(dates_active, sequence_date) Then POW(2, 31 - DATE_DIFF('day',sequence_date, date))
  Else 0
  END) AS BIGINT) As history_int
from today
CROSS JOIN UNNEST (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t(sequence_date)
GROUP BY
user_id
)
Select *,
TO_BASE(history_int,2) As history_in_binary,
TO_BASE(FROM_BASE('11111110000000000000000000000000', 2),2) AS weekly_base,
BIT_COUNT(history_int, 64) AS num_days_active,
BIT_COUNT(BITWISE_AND(history_int,FROM_BASE('11111110000000000000000000000000', 2) ),64) > 0 AS is_weekly_active,
BIT_COUNT(BITWISE_AND(history_int, FROM_BASE('00000001111111000000000000000000',2)),64) > 0 As is_weekly_active_last_week,
BIT_COUNT(BITWISE_AND(history_int,FROM_BASE('11100000000000000000000000000000', 2)),64) > 0 As is_active_last_three_days
FROM date_list_int;

