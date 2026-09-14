INSERT INTO hourly_sensor_summary(
    hour,device_id,avg_energy_kw,max_energy_kw,samples
)
SELECT
    date_trunc('hour',timestamp),
    device_id,
    avg(energy_kw),
    max(energy_kw),
    count(*)
FROM sensor_readings
GROUP BY 1,2
ON CONFLICT(hour,device_id) DO UPDATE SET
    avg_energy_kw=EXCLUDED.avg_energy_kw,
    max_energy_kw=EXCLUDED.max_energy_kw,
    samples=EXCLUDED.samples;
