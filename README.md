# NYC_TAXI_PROYECT

Resumen 

Pipeline de datos completo que ingesta, transforma y modela más de 920 millones de registros del NYC TLC Trip Record Data (2015-2025), implementando arquitectura de medallas (Bronze/Silver/Gold) con dbt, orquestación con Mage, y almacenamiento en Snowflake.

Métricas clave:

135M viajes Green Taxi
784M viajes Yellow Taxi
888M registros en modelo dimensional
3.4% de descarte por reglas de calidad
Clustering con average depth 2.0 (óptimo)

'''
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
│              ANÁLISIS (Jupyter/Colab)                       │
│  data_analysis.ipynb → 5 preguntas de negocio              │
└─────────────────────────────────────────────────────────────┘
