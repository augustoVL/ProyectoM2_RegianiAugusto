"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
with open('postgres_password.txt', 'r', encoding='utf-8') as f:
    postgres_password = f.read().strip()

POSTGRES_CONFIG = {
    'host': '127.0.0.1',
    'database': 'FleetLogix',
    'user': 'postgres',
    'password': postgres_password,
    'port': 5432
}

SNOWFLAKE_CONFIG = {
    'user': 'TEST02',
    'account': 'KDWMDJQ-SN80270',  
    'password': 'Henryptft05', 
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS',
    'private_key_path': 'rsa_key.p8'
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
    
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
        SELECT
            d.delivery_id,
            d.trip_id,
            d.tracking_number,
            d.customer_name,
            d.package_weight_kg,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.delivery_status,
            d.recipient_signature,
            t.vehicle_id,
            t.driver_id,
            t.route_id,
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,
            r.distance_km,
            r.toll_cost,
            r.destination_city
        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE d.delivery_status = 'delivered'
          AND d.delivered_datetime IS NOT NULL
          AND DATE(d.delivered_datetime) = '2026-02-10'


        """
        
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)
            
            # Validaciones de calidad
            # No permitir tiempos negativos
            df = df[df['delivery_time_minutes'] >= 0]
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = '9999-12-31'
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones en Snowflake"""
        logging.info(" Cargando dimensiones...")
        
        cursor = self.sf_conn.cursor()
        
        try:
            # Cargar dim_customer (nuevos clientes)
            cursor.execute("SELECT COALESCE(MAX(customer_key), 0) FROM dim_customer")
            next_customer_key = cursor.fetchone()[0] + 1
            customers = df[['customer_name']].drop_duplicates()
            for _, row in customers.iterrows():
                customer_city = df[df['customer_name'] == row['customer_name']]['destination_city'].iloc[0]
                cursor.execute("""
                    MERGE INTO dim_customer c
                    USING (SELECT %s as customer_key, %s as customer_name, %s as city) s
                    ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN
                        INSERT (customer_key, customer_name, customer_type, city, first_delivery_date, 
                               total_deliveries, customer_category)
                        VALUES (s.customer_key, s.customer_name, 'Individual', s.city, CURRENT_DATE(), 0, 'Regular')
                """, (next_customer_key, row['customer_name'], customer_city))
                next_customer_key += 1
            
            # Actualizar dimensiones SCD Type 2 si hay cambios
            # (Ejemplo simplificado para dim_driver)
            cursor.execute("""
                UPDATE dim_driver 
                SET valid_to = CURRENT_DATE() - 1, is_current = FALSE
                WHERE driver_id IN (
                    SELECT DISTINCT driver_id 
                    FROM staging_daily_load
                ) AND is_current = TRUE
            """)
            
            self.sf_conn.commit()
            logging.info(" Dimensiones actualizadas")
            
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def load_facts(self, df: pd.DataFrame):
        """Cargar hechos en Snowflake"""
        logging.info(" Cargando tabla de hechos...")
        
        cursor = self.sf_conn.cursor()
        
        try:
            # Preparar datos para inserción
            fact_data = []
            for _, row in df.iterrows():
                # Obtener keys de dimensiones
                date_key = int(pd.to_datetime(row['scheduled_datetime']).strftime('%Y%m%d'))
                scheduled_time_key = pd.to_datetime(row['scheduled_datetime']).hour * 100
                delivered_time_key = pd.to_datetime(row['delivered_datetime']).hour * 100
                
                fact_data.append((
                    date_key,
                    scheduled_time_key,
                    delivered_time_key,
                    row['vehicle_id'],  # Simplificado, debería buscar vehicle_key
                    row['driver_id'],   # Simplificado, debería buscar driver_key
                    row['route_id'],    # Simplificado, debería buscar route_key
                    1,  # customer_key placeholder
                    row['delivery_id'],
                    row['trip_id'],
                    row['tracking_number'],
                    row['package_weight_kg'],
                    row['distance_km'],
                    row['fuel_consumed_liters'],
                    row['delivery_time_minutes'],
                    row['delay_minutes'],
                    row['deliveries_per_hour'],
                    row['fuel_efficiency_km_per_liter'],
                    row['cost_per_delivery'],
                    row['revenue_per_delivery'],
                    row['is_on_time'],
                    False,  # is_damaged
                    row['recipient_signature'],
                    row['delivery_status'],
                    self.batch_id
                ))
            
            # Insertar en batch
            cursor.executemany("""
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key, customer_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes, deliveries_per_hour,
                    fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                    is_on_time, is_damaged, has_signature, delivery_status,
                    etl_batch_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, fact_data)
            
            self.sf_conn.commit()
            self.metrics['records_loaded'] = len(fact_data)
            logging.info(f" Cargados {len(fact_data)} registros en fact_deliveries")
            
        except Exception as e:
            logging.error(f" Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes
            self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Crear tabla de totales si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_totals (
                    summary_date DATE,
                    etl_batch_id INTEGER,
                    total_deliveries INTEGER,
                    on_time_deliveries INTEGER,
                    delayed_deliveries INTEGER,
                    total_weight_kg NUMBER(12,2),
                    total_distance_km NUMBER(12,2),
                    total_fuel_liters NUMBER(12,2),
                    total_cost NUMBER(14,2),
                    total_revenue NUMBER(14,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Insertar totales del día
            cursor.execute("""
                INSERT INTO daily_totals (
                    summary_date,
                    etl_batch_id,
                    total_deliveries,
                    on_time_deliveries,
                    delayed_deliveries,
                    total_weight_kg,
                    total_distance_km,
                    total_fuel_liters,
                    total_cost,
                    total_revenue
                )
                SELECT
                    TO_DATE(TO_VARCHAR(date_key), 'YYYYMMDD') AS summary_date,
                    etl_batch_id,
                    COUNT(*) AS total_deliveries,
                    SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
                    SUM(CASE WHEN is_on_time THEN 0 ELSE 1 END) AS delayed_deliveries,
                    COALESCE(SUM(package_weight_kg), 0) AS total_weight_kg,
                    COALESCE(SUM(distance_km), 0) AS total_distance_km,
                    COALESCE(SUM(fuel_consumed_liters), 0) AS total_fuel_liters,
                    COALESCE(SUM(cost_per_delivery), 0) AS total_cost,
                    COALESCE(SUM(revenue_per_delivery), 0) AS total_revenue
                FROM fact_deliveries
                WHERE etl_batch_id = %s
                GROUP BY date_key, etl_batch_id
            """, (self.batch_id,))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()
