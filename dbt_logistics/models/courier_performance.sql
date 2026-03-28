SELECT 
    courier_id,
    AVG(speed) as avg_speed,
    COUNT(*) as violation_count,
    MAX(event_time) as last_violation
FROM public.high_risk_alerts
GROUP BY courier_id