WITH aggregated_data AS (
    SELECT 
        courier_id,
        AVG(avg_speed) AS avg_speed,            
        SUM(violation_count) AS violation_count, 
        MAX(last_violation) AS last_violation   
    FROM public_analytics.courier_performance
    GROUP BY courier_id                          
)

SELECT 
    *,
    CASE 
        WHEN avg_speed > 135 OR violation_count > 60 THEN 'Extreme Risk'
        WHEN avg_speed BETWEEN 115 AND 135 THEN 'High Risk'           
        WHEN avg_speed BETWEEN 90 AND 115 THEN 'Moderate'             
        WHEN avg_speed BETWEEN 70 AND 90 THEN 'Low Risk'              
        ELSE 'Safe'                                                   
    END AS risk_level,

    CASE 
        WHEN violation_count > 40 THEN 'Critical: Needs Training'     
        WHEN violation_count BETWEEN 15 AND 40 THEN 'Warning: Review'  
        WHEN violation_count < 5 AND avg_speed < 85 THEN 'Top Performer'
        ELSE 'Active'                                                 
    END AS performance_status,

    (avg_speed - 100) AS speed_deviation,

    CASE 
        WHEN avg_speed < 90 AND violation_count = 0 THEN 'Eligible for Bonus'
        ELSE 'Not Eligible'
    END AS bonus_status

FROM aggregated_data