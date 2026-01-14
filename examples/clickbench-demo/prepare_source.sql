ALTER TABLE `clickbench/hits`
ADD COLUMN last_modified_at Timestamp;

-- Заполняем last_modified_at: случайное время от event_time до event_time + 2 часа
UPDATE `clickbench/hits`
SET last_modified_at = 
    DateTime::FromSeconds(
        DateTime::ToSeconds(EventTime) + 
        CAST(Random(WatchID) * 7200 AS Uint32)
    );


-- Позже: имитируем апдейт 10% строк — например, при донастройке данных
UPDATE `clickbench/hits`
SET last_modified_at = CurrentUtcTimestamp()
WHERE CAST(Random(WatchID) * 100 AS Uint32) < 10; 

select 
lmd, count(*)
 from `clickbench/hits`
 group by CAST(CAST(last_modified_at AS Date) AS String) as lmd

select 
 * 
FROM `clicks_dbt_demo_02_base/base_clicks`
WHERE last_modified_at > CAST('2013-07-15 21:56:21' AS Timestamp);