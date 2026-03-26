
#!/bin/bash
# Script de migración PostgreSQL local -> RDS

# Variables
LOCAL_DB="fleetlogix"
LOCAL_USER="postgres"
RDS_ENDPOINT="fleetlogix-db.xxxx.us-east-1.rds.amazonaws.com"
RDS_USER="fleetlogix_admin"
RDS_DB="fleetlogix"

echo " Iniciando migración de base de datos..."

# 1. Hacer dump de la base local
echo " Exportando base de datos local..."
pg_dump -h localhost -U $LOCAL_USER -d $LOCAL_DB -f fleetlogix_dump.sql

# 2. Crear base de datos en RDS
echo " Creando base de datos en RDS..."
psql -h $RDS_ENDPOINT -U $RDS_USER -c "CREATE DATABASE $RDS_DB;"

# 3. Restaurar en RDS
echo " Importando datos en RDS..."
psql -h $RDS_ENDPOINT -U $RDS_USER -d $RDS_DB -f fleetlogix_dump.sql

echo " Migración completada"
