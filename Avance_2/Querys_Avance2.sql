--Query 1: vehículos con más kilómetros recorridos
EXPLAIN ANALYZE
SELECT 
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    SUM(r.distance_km) AS total_km_recorridos
FROM trips t
JOIN vehicles v ON t.vehicle_id = v.vehicle_id
JOIN routes r ON t.route_id = r.route_id
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type
ORDER BY total_km_recorridos DESC
LIMIT 10;


--Query 2: conductores con más kilómetros recorridos
EXPLAIN ANALYZE
SELECT 
    d.driver_id,
    d.first_name,
    d.last_name,
    SUM(r.distance_km) AS total_km_recorridos
FROM trips t
JOIN drivers d ON t.driver_id = d.driver_id
JOIN routes r ON t.route_id = r.route_id
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY total_km_recorridos DESC
LIMIT 10;


--Query 3: vehículos con mayor costo de mantenimiento
EXPLAIN ANALYZE
SELECT 
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    SUM(m.cost) AS costo_total_mantenimiento
FROM maintenance m
JOIN vehicles v ON m.vehicle_id = v.vehicle_id
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type
ORDER BY costo_total_mantenimiento DESC
LIMIT 10;


-- Query 4: Promedio de entregas por conductor en los últimos 6 meses
EXPLAIN ANALYZE
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name AS conductor,
    COUNT(del.delivery_id) AS total_entregas,
    COUNT(DISTINCT t.trip_id) AS total_viajes,
    ROUND(COUNT(del.delivery_id)::numeric / NULLIF(COUNT(DISTINCT t.trip_id), 0), 2) AS promedio_entregas_por_viaje
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
JOIN deliveries del ON t.trip_id = del.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY d.driver_id, d.first_name, d.last_name
HAVING COUNT(del.delivery_id) > 0
ORDER BY promedio_entregas_por_viaje DESC
LIMIT 10;


-- Query Intermedia 2: Rutas con mayor cantidad de viajes completados
EXPLAIN ANALYZE
SELECT 
    r.route_id,
    r.origin_city,
    r.destination_city,
    COUNT(t.trip_id) AS total_viajes,
    AVG(t.fuel_consumed_liters) AS promedio_combustible,
    AVG(t.total_weight_kg) AS promedio_carga
FROM routes r
JOIN trips t ON r.route_id = t.route_id
WHERE t.status = 'completed'
GROUP BY r.route_id, r.origin_city, r.destination_city
HAVING COUNT(t.trip_id) >= 10
ORDER BY total_viajes desc
limit 10;

--Query Intermedia 3: Rutas con mayor cantidad de entregas demoradas
EXPLAIN ANALYZE
SELECT 
    r.route_id,
    r.origin_city,
    r.destination_city,
    COUNT(del.delivery_id) AS total_entregas,
    SUM(CASE 
        WHEN del.delivered_datetime > del.scheduled_datetime THEN 1
        ELSE 0
    END) AS entregas_demoradas
FROM routes r
JOIN trips t ON r.route_id = t.route_id
JOIN deliveries del ON t.trip_id = del.trip_id
WHERE del.delivered_datetime IS NOT NULL
GROUP BY r.route_id, r.origin_city, r.destination_city
HAVING COUNT(del.delivery_id) >= 20
ORDER BY entregas_demoradas desc
limit 10;


## Query Intermedia 4: Cantidad de viajes completados por mes

EXPLAIN ANALYZE
SELECT 
    DATE_TRUNC('month', departure_datetime) AS mes,
    COUNT(*) AS total_viajes
FROM trips
WHERE status = 'completed'
GROUP BY DATE_TRUNC('month', departure_datetime)
ORDER BY mes;



-- Query Intermedia 5: Ciudades destino con mayor peso promedio entregado
EXPLAIN ANALYZE
SELECT 
    r.destination_city,
    COUNT(del.delivery_id) AS total_entregas,
    AVG(del.package_weight_kg) AS peso_promedio_paquetes
FROM deliveries del
JOIN trips t ON del.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
GROUP BY r.destination_city
HAVING COUNT(del.delivery_id) >= 50
ORDER BY peso_promedio_paquetes DESC;


--Query Compleja 1: Ranking de eficiencia de rutas considerando tiempo, combustible y entregas exitosas
explain analyze
WITH route_metrics AS (
    SELECT 
        r.route_id,
        r.origin_city,
        r.destination_city,
        COUNT(DISTINCT t.trip_id) AS total_viajes,
        AVG(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600) AS promedio_horas,
        AVG(t.fuel_consumed_liters) AS promedio_combustible,
        COUNT(del.delivery_id) AS total_entregas,
        SUM(CASE 
            WHEN del.delivery_status = 'delivered' THEN 1
            ELSE 0
        END) AS entregas_exitosas
    FROM routes r
    JOIN trips t ON r.route_id = t.route_id
    LEFT JOIN deliveries del ON t.trip_id = del.trip_id
    WHERE t.status = 'completed'
      AND t.arrival_datetime IS NOT NULL
    GROUP BY r.route_id, r.origin_city, r.destination_city
),
route_efficiency AS (
    SELECT 
        route_id,
        origin_city,
        destination_city,
        total_viajes,
        ROUND(promedio_horas::numeric, 2) AS promedio_horas,
        ROUND(promedio_combustible::numeric, 2) AS promedio_combustible,
        total_entregas,
        entregas_exitosas,
        ROUND((entregas_exitosas * 100.0 / NULLIF(total_entregas, 0))::numeric, 2) AS porcentaje_exito
    FROM route_metrics
)
SELECT 
    route_id,
    origin_city,
    destination_city,
    total_viajes,
    promedio_horas,
    promedio_combustible,
    total_entregas,
    entregas_exitosas,
    porcentaje_exito,
    RANK() OVER (
        ORDER BY porcentaje_exito DESC, promedio_horas ASC, promedio_combustible ASC
    ) AS ranking_eficiencia
FROM route_efficiency
ORDER BY ranking_eficiencia;

-- Query Compleja 2: Evolución semestral de viajes entre ciudades con comparación contra el semestre anterior

EXPLAIN ANALYZE
WITH viajes_por_semestre AS (
    SELECT 
        r.origin_city,
        r.destination_city,
        DATE_TRUNC('year', t.departure_datetime) +
        CASE
            WHEN EXTRACT(MONTH FROM t.departure_datetime) <= 6
                THEN INTERVAL '0 months'
            ELSE INTERVAL '6 months'
        END AS semestre,
        COUNT(t.trip_id) AS total_viajes
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    WHERE t.status = 'completed'
    GROUP BY 
        r.origin_city,
        r.destination_city,
        DATE_TRUNC('year', t.departure_datetime) +
        CASE
            WHEN EXTRACT(MONTH FROM t.departure_datetime) <= 6
                THEN INTERVAL '0 months'
            ELSE INTERVAL '6 months'
        END
),
comparacion_semestral AS (
    SELECT 
        origin_city,
        destination_city,
        semestre,
        total_viajes,
        LAG(total_viajes) OVER (
            PARTITION BY origin_city, destination_city
            ORDER BY semestre
        ) AS viajes_semestre_anterior
    FROM viajes_por_semestre
)
SELECT 
    origin_city,
    destination_city,
    semestre,
    total_viajes,
    viajes_semestre_anterior,
    total_viajes - viajes_semestre_anterior AS variacion_absoluta,
    ROUND(
        ((total_viajes - viajes_semestre_anterior) * 100.0 / NULLIF(viajes_semestre_anterior, 0))::numeric,
        2
    ) AS variacion_porcentual
FROM comparacion_semestral
ORDER BY origin_city, destination_city, semestre;





--Query Compleja 3: Ranking de ciudades destino según entregas exitosas y participación sobre el total

EXPLAIN ANALYZE
WITH entregas_por_ciudad AS (
    SELECT 
        r.destination_city,
        COUNT(del.delivery_id) AS total_entregas,
        SUM(CASE 
            WHEN del.delivery_status = 'delivered' THEN 1
            ELSE 0
        END) AS entregas_exitosas
    FROM deliveries del
    JOIN trips t ON del.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    GROUP BY r.destination_city
),
participacion_ciudad AS (
    SELECT 
        destination_city,
        total_entregas,
        entregas_exitosas,
        ROUND((entregas_exitosas * 100.0 / NULLIF(total_entregas, 0))::numeric, 2) AS porcentaje_exito,
        ROUND((total_entregas * 100.0 / SUM(total_entregas) OVER ())::numeric, 2) AS participacion_total
    FROM entregas_por_ciudad
)
SELECT 
    destination_city,
    total_entregas,
    entregas_exitosas,
    porcentaje_exito,
    participacion_total,
    RANK() OVER (ORDER BY entregas_exitosas DESC) AS ranking_ciudad
FROM participacion_ciudad
ORDER BY ranking_ciudad;


--- Query Compleja 4: Ranking de tipos de vehículo según uso operativo y costo de mantenimiento

explain analyze
WITH metricas_por_vehiculo AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_type,
        COUNT(DISTINCT t.trip_id) AS total_viajes,
        COALESCE(SUM(r.distance_km), 0) AS km_totales,
        COALESCE(SUM(m.cost), 0) AS costo_mantenimiento_total
    FROM vehicles v
    LEFT JOIN trips t ON v.vehicle_id = t.vehicle_id AND t.status = 'completed'
    LEFT JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
    GROUP BY v.vehicle_id, v.vehicle_type
),
resumen_por_tipo AS (
    SELECT 
        vehicle_type,
        COUNT(vehicle_id) AS cantidad_vehiculos,
        SUM(total_viajes) AS viajes_totales,
        SUM(km_totales) AS km_totales,
        SUM(costo_mantenimiento_total) AS costo_total_mantenimiento,
        ROUND(
            (SUM(costo_mantenimiento_total) / NULLIF(SUM(km_totales), 0))::numeric,
            2
        ) AS costo_por_km
    FROM metricas_por_vehiculo
    GROUP BY vehicle_type
)
SELECT 
    vehicle_type,
    cantidad_vehiculos,
    viajes_totales,
    km_totales,
    costo_total_mantenimiento,
    costo_por_km,
    RANK() OVER (ORDER BY costo_por_km ASC) AS ranking_eficiencia_costo
FROM resumen_por_tipo
ORDER BY ranking_eficiencia_costo;
