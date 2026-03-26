-- =====================================================
-- FLEETLOGIX - ÍNDICES DE OPTIMIZACIÓN
-- Basados en las queries analizadas del Avance 2
-- Objetivo: mejorar el acceso en JOINs y filtros frecuentes
-- =====================================================

-- =====================================================
-- ÍNDICE 1: Filtrado por estado y fecha en trips
-- =====================================================
-- Justificación: varias queries trabajan con viajes completados
-- y también usan departure_datetime para filtrar o agrupar.
-- Queries beneficiadas: Intermedia 1, Intermedia 2, Intermedia 4,
-- Compleja 1, Compleja 2
CREATE INDEX idx_trips_status_departure
ON trips(status, departure_datetime);

-- =====================================================
-- ÍNDICE 2: Relación entre rutas y viajes según estado
-- =====================================================
-- Justificación: varias queries unen routes con trips por route_id
-- y además filtran por status = 'completed'.
-- Queries beneficiadas: Intermedia 2, Intermedia 3,
-- Compleja 1, Compleja 2
CREATE INDEX idx_trips_route_status
ON trips(route_id, status);

-- =====================================================
-- ÍNDICE 3: Análisis temporal por conductor
-- =====================================================
-- Justificación: se usa driver_id para relacionar conductores
-- con viajes, y departure_datetime para trabajar por período.
-- Queries beneficiadas: Intermedia 1, Query 2
CREATE INDEX idx_trips_driver_departure
ON trips(driver_id, departure_datetime);

-- =====================================================
-- ÍNDICE 4: Relación entre deliveries y trips
-- =====================================================
-- Justificación: muchas queries unen deliveries con trips
-- mediante trip_id, por lo que esta columna es clave.
-- Queries beneficiadas: Intermedia 1, Intermedia 3,
-- Intermedia 5, Compleja 1, Compleja 3
CREATE INDEX idx_deliveries_trip_id
ON deliveries(trip_id);

-- =====================================================
-- ÍNDICE 5: Relación entre maintenance y vehicles
-- =====================================================
-- Justificación: las consultas de mantenimiento se apoyan
-- en la relación por vehicle_id.
-- Queries beneficiadas: Query 3, Compleja 4
CREATE INDEX idx_maintenance_vehicle_id
ON maintenance(vehicle_id);

-- =====================================================
-- VERIFICACIÓN DE ÍNDICES CREADOS
-- =====================================================
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- =====================================================
-- ACTUALIZACIÓN DE ESTADÍSTICAS
-- =====================================================
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;

-- Observación:
-- Aunque los índices fueron creados sobre columnas importantes para joins
-- y filtros frecuentes, no en todos los casos se observó una mejora clara
-- en el tiempo de ejecución. Esto puede suceder porque la mayoría de las
-- queries del trabajo son analíticas: recorren muchas filas y realizan
-- agregaciones, promedios, conteos, agrupamientos y rankings. En esos casos,
-- PostgreSQL puede preferir un Seq Scan antes que usar un índice, ya que
-- igualmente necesita leer gran parte de la tabla para construir el resultado.
