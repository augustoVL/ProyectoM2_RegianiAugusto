-- =====================================================
-- FLEETLOGIX DATABASE SETUP
-- Sistema de Gestión de Transporte y Logística
-- =====================================================

-- 1. Crear las tablas del modelo relacional

-- Tabla 1: vehicles (vehículos de la flota)
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

-- Tabla 2: drivers (conductores)
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    employee_code VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    license_number VARCHAR(50) UNIQUE NOT NULL,
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

-- Tabla 3: routes (rutas predefinidas)
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_code VARCHAR(20) UNIQUE NOT NULL,
    origin_city VARCHAR(100) NOT NULL,
    destination_city VARCHAR(100) NOT NULL,
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2) DEFAULT 0
);

-- Tabla 4: trips (viajes realizados)
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    driver_id INTEGER REFERENCES drivers(driver_id),
    route_id INTEGER REFERENCES routes(route_id),
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP,
    fuel_consumed_liters DECIMAL(10,2),
    total_weight_kg DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'in_progress'
);

-- Tabla 5: deliveries (entregas individuales)
CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    trip_id INTEGER REFERENCES trips(trip_id),
    tracking_number VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    delivery_address TEXT NOT NULL,
    package_weight_kg DECIMAL(10,2),
    scheduled_datetime TIMESTAMP,
    delivered_datetime TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending',
    recipient_signature BOOLEAN DEFAULT FALSE
);

-- Tabla 6: maintenance (mantenimientos de vehículos)
CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(10,2),
    next_maintenance_date DATE,
    performed_by VARCHAR(200)
);

-- 2. Crear índices básicos proporcionados
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);

-- 3. Agregar comentarios a las tablas para documentación
COMMENT ON TABLE vehicles IS 'Registro de vehículos de la flota de FleetLogix';
COMMENT ON TABLE drivers IS 'Información de conductores empleados';
COMMENT ON TABLE routes IS 'Rutas predefinidas entre ciudades';
COMMENT ON TABLE trips IS 'Registro de viajes realizados';
COMMENT ON TABLE deliveries IS 'Entregas individuales asociadas a cada viaje';
COMMENT ON TABLE maintenance IS 'Historial de mantenimiento de vehículos';

-- 4. Verificar la creación de las tablas
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'public' 
     AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- 5. Verificar las relaciones (foreign keys)
SELECT
    tc.table_name AS tabla_origen,
    kcu.column_name AS columna_origen,
    ccu.table_name AS tabla_referencia,
    ccu.column_name AS columna_referencia
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
AND tc.table_schema = 'public';

-- 6. Verificar índices creados
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;


---Query básica 1: filtro y ordenamiento

SELECT 
    t.trip_id,
    t.departure_datetime,
    t.arrival_datetime,
    t.status AS trip_status,
    v.license_plate,
    v.vehicle_type,
    d.first_name,
    d.last_name
FROM trips t
JOIN vehicles v 
    ON t.vehicle_id = v.vehicle_id
JOIN drivers d 
    ON t.driver_id = d.driver_id
ORDER BY t.departure_datetime DESC
LIMIT 50;

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



-- 2. Crear índices básicos proporcionados
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);

--- INDEX CREADOS
CREATE INDEX idx_trips_status_departure
ON trips(status, departure_datetime);

CREATE INDEX idx_trips_route_status
ON trips(route_id, status);

CREATE INDEX idx_trips_driver_departure
ON trips(driver_id, departure_datetime);

CREATE INDEX idx_deliveries_trip_id
ON deliveries(trip_id);

CREATE INDEX idx_maintenance_vehicle_id
ON maintenance(vehicle_id);


