WITH raw_data AS (
    SELECT 
        courier_id,
        avg_speed,
        violation_count,
        last_violation
    FROM {{ source('my_raw_data', 'courier_performance') }} -- ده الجدول اللي سبارك رماه
)

SELECT 
    *,
   
    CASE 
        WHEN avg_speed > 110 OR violation_count > 50 THEN 'Extreme Risk'
        WHEN avg_speed BETWEEN 90 AND 110 THEN 'High Risk'
        WHEN avg_speed BETWEEN 70 AND 90 THEN 'Moderate'
        ELSE 'Safe'
    END AS risk_level,

    CASE 
        WHEN violation_count > 30 THEN 'Needs Training'
        WHEN violation_count < 5 AND avg_speed < 80 THEN 'Top Performer'
        ELSE 'Active'
    END AS performance_status,

    (avg_speed - 90) AS speed_deviation

FROM raw_data