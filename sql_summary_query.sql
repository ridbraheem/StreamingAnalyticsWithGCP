SELECT 
Hour
,ActivityPerHour_Create AS Creates
,ActivityPerHour_View AS Views
,ActivityPerHour_Modify AS Modifies
,ActivityPerHour_Delete AS Deletes
,ActivityPerHour_Create + ActivityPerHour_View + ActivityPerHour_Modify + ActivityPerHour_Delete AS TotalActivities
FROM (
    SELECT 
    r.Hour
    ,r.activity 
    ,COUNT(r.activity) ActivityPerHour
    FROM(
        SELECT 
        eventTime
        ,FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",DATETIME_TRUNC(DATETIME (eventTime), Hour)) as Hour
        ,activity
        FROM `PROJECT.DATASET.raw_activity_table` 
        WHERE DATE(processingTime) = CURRENT_DATE() LIMIT 100000
    ) r
    GROUP BY r.Hour,r.activity)
    PIVOT
(
  SUM(ActivityPerHour) AS ActivityPerHour
  FOR activity in ('Create', 'View', 'Modify', 'Delete','NULL')
)
ORDER BY Hour ASC
