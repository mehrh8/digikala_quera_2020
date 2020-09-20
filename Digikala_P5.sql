SELECT
       L2.user_id AS user_id,
       L1.date AS start_date,
       L2.date AS end_date,
       L2.log AS status,
       CAST(JulianDay(date(L2.date)) - JulianDay(date(L1.date))+1 AS integer) AS length
FROM (SELECT
       *,
       ROW_NUMBER () OVER () RowNum
FROM (SELECT
       date,
       user_id,
       log,
       log - LAG ( log, 1, ABS(log-1) ) OVER (PARTITION BY user_id)  AS log_LAG,
       log - LEAD ( log,1, ABS(log-1)) OVER (PARTITION BY user_id) AS log_LEAD

FROM users_log)
WHERE log_LAG=1 OR log_LAG=-1) L1
CROSS JOIN  (
    SELECT
       *,
       ROW_NUMBER () OVER () RowNum
FROM (SELECT
       date,
       user_id,
       log,
       log - LAG ( log, 1, ABS(log-1) ) OVER (PARTITION BY user_id)  AS log_LAG,
       log - LEAD ( log,1, ABS(log-1)) OVER (PARTITION BY user_id) AS log_LEAD

FROM users_log)
WHERE log_LEAD=1 OR log_LEAD=-1) L2 ON L1.RowNum=L2.RowNum
ORDER BY start_date;
