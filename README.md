# NYC_TAXI_PROYECT

Resumen 

Pipeline de datos completo que ingesta, transforma y modela más de 920 millones de registros del NYC TLC Trip Record Data (2015-2025), implementando arquitectura de medallas (Bronze/Silver/Gold) con dbt, orquestación con Mage, y almacenamiento en Snowflake.

Métricas clave:

135M viajes Green Taxi
784M viajes Yellow Taxi
888M registros en modelo dimensional
3.4% de descarte por reglas de calidad
Clustering con average depth 2.0 (óptimo)



Arquitectura
```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTA (Mage)                           │
│  NYC TLC Parquet Files (2015-2025) → Snowflake Bronze      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                 TRANSFORMACIÓN (dbt)                         │
│                                                              │
│  BRONZE (Raw)                                               │
│  ├── green_trips (135M)                                     │
│  ├── yellow_trips (784M)                                    │
│  └── taxi_zones (265)                                       │
│                              ↓                               │
│  SILVER (Staged/Curated)                                    │
│  ├── green_trips → estandarización                          │
│  ├── yellow_trips → estandarización                         │
│  ├── stg_all_trips → unificación (920M)                    │
│  └── trips_enriched → enriquecimiento con zonas            │
│                              ↓                               │
│  GOLD (Dimensional Model)                                   │
│  ├── fct_trips (888M) [CLUSTERED]                          │
│  ├── dim_date (3,946)                                       │
│  ├── dim_zone (265)                                         │
│  └── dim_payment_type (5)                                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              ANÁLISIS (Colab)                       │
│  data_analysis.ipynb → 5 preguntas de negocio              │
└─────────────────────────────────────────────────────────────┘
```
Pipeline ETL que ingesta 920M+ registros de NYC TLC (2015-2025) implementando arquitectura de medallas.
Flujo detallado:

Bronze: green_trips (135M), yellow_trips (784M), taxi_zones (265)
Silver: Conversión timestamps, unificación Yellow+Green, enriquecimiento con zonas
Gold: fct_trips (888M), dim_date (3,946), dim_zone (265), dim_payment_type (5)
```
Parquet Files → MAGE (ingesta) → BRONZE (raw) 
                                     ↓
                                  dbt run
                                     ↓
                    SILVER (estandarización/enriquecimiento)
                                     ↓
                    GOLD (modelo dimensional + clustering)
```
Cobertura de datos 
Green
```
green	2015	38467530
green	2016	32771082
green	2017	23473812
green	2018	17798628
green	2019	12601628
green	2020	3468332
green	2021	2137458
green	2022	1680788
green	2023	1574110
green	2024	1320402
green	2025	397917    
```
Yellow
```
yellow	2015	146039232
yellow	2016	131131805
yellow	2017	113500386
yellow	2018	102870524
yellow	2019	84597309
yellow	2020	24649266
yellow	2021	30903983
yellow	2022	39655622
yellow	2023	38310138
yellow	2024	41169691
yellow	2025	31556416

```


ESTRATEGIA DE BACKFILL



Pipeline: load_green_trips y load_yellow_trips
Chunking mensual: 1 archivo Parquet por ejecución
Idempotencia: Columnas _ingest_year, _ingest_month, _source_file previenen duplicados
Metadatos por lote: Timestamp, run_id, conteos por batch



GESTION DE SECRETOS 
```

| Nombre del Secreto    | Propósito                               |
| --------------------- | --------------------------------------- |
| `snowflake_account`   | Cuenta de Snowflake usada para conexión |
| `snowflake_user`      | Usuario de servicio                     |
| `snowflake_password`  | Password asociado                       |
| `snowflake_role`      | Rol con permisos mínimos                |
| `snowflake_warehouse` | Warehouse para ejecutar queries         |
| `snowflake_database`  | Base de datos destino                   |
```
Cuenta de servicio / Rol: solo con permisos mínimos para:

USAGE en warehouse y database.

CREATE/SELECT en esquemas bronze, silver, gold.

No se otorgan permisos de DROP fuera de su scope.




DISEÑO SILVER






Reglas de limpieza y estandarización:

Conversión de campos de fecha/hora a TIMESTAMP_NTZ.

Normalización de payment_type con códigos consistentes.

Conversión de distancias y tarifas a FLOAT.

Eliminación de duplicados (ROW_NUMBER() OVER ...).

Validación de rangos de datos (ej. trip_distance > 0).

Reglas de calidad aplicadas:

NOT NULL en fechas, distancia, tarifa
Sin validación de rangos (preservar datos originales)









DISEÑO GOLD





Modelos Dimensionales:

Dim_Date: calendario (día, mes, año, semana, trimestre).

Dim_Zone: zonas de origen/destino (borough, zone_name).

Dim_Payment_Type: catálogo de métodos de pago.

Tabla de Hechos:

Fct_Trips

Keys: pickup_date_sk, dropoff_date_sk, pickup_zone_sk, dropoff_zone_sk, payment_type_sk.







Métricas:

passenger_count

trip_distance

fare_amount

tip_amount

total_amount

trip_duration_minutes

avg_speed_mph

Registros: 888,476,582
Granularidad: 1 fila = 1 viaje









CLUSTERING EN SNOWFLAKE
Configuracion aplicada

ALTER TABLE SILVER_GOLD.fct_trips 
CLUSTER BY (pickup_date_sk, service_type);

Justificación de Cluster Keys







pickup_date_sk:

Patrón de consulta más común: filtros temporales (por mes, año, trimestre)
Alta cardinalidad: 3,946 valores únicos
Mejora pruning de particiones en queries con WHERE fecha





service_type:

Baja cardinalidad: 2 valores ('yellow', 'green')
Análisis frecuentes por tipo de servicio
Complementa el filtrado temporal






Resultados antes/después:

Antes: consultas filtrando por fechas escaneaban 100% de particiones.

Después: reducción de ~70% en tiempos de consulta.


Conclusiones

Clustering efectivo con depth óptimo
Queries con filtros temporales se benefician significativamente









Pruebas

Calidad de Datos
Tests dbt Implementados


Total: 25 tests ejecutados, 25 pasando
Auditoria de registros 
Análisis de calidad:

Bronze → Silver (Green): 135,693,276 → 135,693,247 = 29 registros descartados (0.00002%)
Bronze → Silver (Yellow): 784,387,076 → 784,387,028 = 48 registros descartados (0.00001%)
Silver → Gold: 920,080,275 → 888,476,582 = 31,603,693 registros descartados (3.4%)

El descarte de Bronze a Silver es casi cero, pero de Silver a Gold se perdió el 3.4%
Posibles razones 


Fechas inválidas (nulls en pickup/dropoff)
Joins fallidos con dimensiones
Filtros en el WHERE de fct_trips




TROUBLESHOOTING



Problemas Comunes
1. Error: "invalid identifier 'LPEP_PICKUP_DATETIME'"
Causa: Nombres de columnas en Snowflake con case sensitivity.



CHECKLIST 
OK  Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green; matriz de
cobertura en README. NYC.gov
OK  Mage orquesta backfill mensual con idempotencia y metadatos por lote.
OK  Bronze (raw) refleja fielmente el origen; Silver unifica/escaliza; Gold en estrella con
fct_trips y dimensiones clave.
OK  Clustering aplicado a fct_trips con evidencia antes/después (Query Profile,
pruning). Snowflake Docs
OK  Secrets y cuenta de servicio con permisos mínimos (evidencias sin exponer valores).
OK  Tests dbt (not_null, unique, accepted_values, relationships) pasan; docs y lineage
generados.
OK  Notebook con respuestas a las 5 preguntas de negocio desde gold.
