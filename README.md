# 🚛 FleetLogix: Ecosistema de Datos y Logística Serverless

**FleetLogix** es una plataforma integral de ingeniería de datos diseñada para optimizar la gestión de flotas, el seguimiento de entregas en tiempo real y el análisis avanzado de métricas logísticas. El proyecto abarca desde la generación de datos sintéticos masivos hasta un pipeline ETL automatizado y arquitectura cloud serverless.

## Arquitectura del Proyecto

El sistema se divide en tres capas principales:

### 1. Generación y Almacenamiento Relacional (SQL)
- **Motor:** PostgreSQL.
- **Generación de Datos:** Script especializado (`A1-01`) que genera más de **505,000 registros** (vehículos, conductores, rutas, viajes y entregas) manteniendo integridad referencial.
- **Esquema:** Diseño de base de datos optimizado con índices y vistas para análisis de eficiencia de costos.
-**Documentacion** Dentro de la carpeta Avance_1, se encuentran los archivos que documentan el trabajo realizado

### 2. Pipeline ETL y Data Warehousing
- **Flujo:** Extracción desde PostgreSQL local, transformación de datos y carga (ELT/ETL) en **Snowflake**.
- **Automatización:** Ejecución programada (Python `schedule`) para procesamiento diario a las 02:00 AM.
- **Logs:** Sistema de monitoreo de batch IDs para asegurar la trazabilidad de la carga.
-**Documentacion** Dentro de la carpeta Avance_2, se encuentran los archivos que documentan el trabajo realizado

### 3. Infraestructura Cloud y Procesamiento Real-Time (AWS)
- **Provisionamiento:** Script de automatización con `Boto3` para crear la infraestructura en `us-east-1` / `us-east-2`.
- **Servicios Cloud:**
  - **DynamoDB:** 4 tablas para tracking y estados de entregas en tiempo real.
  - **S3:** Bucket para almacenamiento de reportes y auditoría.
  - **AWS Lambda:** Funciones para verificación de entregas, cálculo de ETA y alertas de desvío de ruta.
  - **IAM:** Roles con políticas de mínimo privilegio para la ejecución de servicios.
  -**Documentacion** Dentro de la carpeta Avance_3, y avance_4, se encuentran los archivos que documentan el trabajo realizado

##  Estructura del Repositorio

| Archivo | Descripción |
| :--- | :--- |
| `A1-01_data_generation_estudiantes.py` | Motor de simulación de datos masivos. |
| `fleetlogix_db_schema.sql` | Definición de tablas, índices y vistas SQL. |
| `A3-05_etl_pipeline_estudiantes.py` | Pipeline de integración PostgreSQL -> Snowflake. |
| `A4-06_aws_setup.py` | Script de despliegue de infraestructura en AWS. |
| `A4-lambda_functions.py` | Lógica de negocio serverless para procesamiento de eventos. |

## 🚀 Desafíos Superados
- **Integridad de Datos:** Manejo de claves foráneas y reglas de negocio en simulaciones de gran volumen.
- **Consistencia Regional:** Resolución de conflictos de despliegue entre regiones de AWS (`us-east-1` vs `us-east-2`).
- **Seguridad Cloud:** Implementación de variables de entorno y archivos de configuración para evitar la exposición de credenciales.

## 🛠️ Requisitos
- Python 3.11+
- PostgreSQL / Snowflake Account
- AWS CLI configurado
- Librerías: `boto3`, `psycopg2`, `pandas`, `snowflake-connector-python`, `faker`.
