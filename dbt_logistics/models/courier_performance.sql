WITH aggregated_data AS (
    SELECT 
        courier_id,
        AVG(avg_speed) AS avg_speed,             -- بيحسب متوسط السرعة لكل سواق
        SUM(violation_count) AS violation_count, -- بيجمع إجمالي المخالفات
        MAX(last_violation) AS last_violation    -- بياخد وقت آخر مخالفة حصلت
    FROM {{ source('my_raw_data', 'courier_performance') }}
    GROUP BY courier_id                          -- أهم سطر: بيمنع التكرار نهائياً
)

SELECT 
    *,
    -- حساب مستوى الخطورة بناءً على البيانات المجمعة
    CASE 
        WHEN avg_speed > 110 OR violation_count > 50 THEN 'Extreme Risk'
        WHEN avg_speed BETWEEN 90 AND 110 THEN 'High Risk'
        WHEN avg_speed BETWEEN 70 AND 90 THEN 'Moderate'
        ELSE 'Safe'
    END AS risk_level,

    -- حساب حالة الأداء
    CASE 
        WHEN violation_count > 30 THEN 'Needs Training'
        WHEN violation_count < 5 AND avg_speed < 80 THEN 'Top Performer'
        ELSE 'Active'
    END AS performance_status,

    -- حساب الانحراف عن السرعة المسموحة (افترضنا 90)
    (avg_speed - 90) AS speed_deviation

FROM aggregated_data