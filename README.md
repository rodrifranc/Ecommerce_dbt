# 🚀 Pipeline ELT: Maven Fuzzy Factory E-Commerce

Este proyecto implementa un *Modern Data Stack* completo para "Maven Fuzzy Factory", un e-commerce ficticio de juguetes. El pipeline extrae datos de una base de datos transaccional, los carga en un Data Warehouse en la nube, transforma los datos analíticos y los visualiza en un dashboard interactivo, todo orquestado de forma automatizada.

Proyecto desarrollado como parte de la **Maestría en Inteligencia Artificial (MIA 03)** - Facultad Politécnica, Universidad Nacional de Asunción.

## 🏗️ Arquitectura del Proyecto

El flujo de datos (ELT) utiliza las siguientes herramientas:
* **Fuente de Datos:** MySQL (Base de datos transaccional con ~1.7M de registros).
* **Extract & Load (EL):** Airbyte (Sincronización hacia MotherDuck).
* **Data Warehouse:** MotherDuck / DuckDB.
* **Transform (T):** dbt (Data Build Tool) para crear modelos de Staging y Marts.
* **Orquestación:** Prefect (Script en Python gestionando la ejecución secuencial).
* **Visualización:** Metabase (Dashboard analítico alojado en Docker).

## 📊 Modelo de Datos

Los datos analizados incluyen sesiones web, páginas vistas, catálogo de productos, órdenes de compra y reembolsos. A través de **dbt**, los datos crudos fueron transformados en una arquitectura de capas:
1. **Capa Staging (`main_staging`):** Limpieza y estandarización de las fuentes (`stg_sessions`, `stg_orders`, `stg_order_items`).
2. **Capa Marts (`main_marts`):** Modelos de negocio listos para consumo, incluyendo ventas diarias (`fct_daily_sales`), rendimiento por canal (`fct_channel_performance`), y una tabla desnormalizada One-Big-Table (`obt_orders_enriched`).

## ⚙️ Configuración y Ejecución

### 1. Variables de Entorno
Crea un archivo `.env` en la raíz del proyecto con las credenciales de Airbyte:

```env
AIRBYTE_HOST=localhost
AIRBYTE_PORT=8000
AIRBYTE_USERNAME=tu_usuario
AIRBYTE_PASSWORD=tu_password
AIRBYTE_CONNECTION_ID=tu_connection_id
```

### 2. Entorno Virtual y Dependencias

```bash
python -m venv venv
.\venv\Scripts\activate
pip install dbt-duckdb prefect httpx python-dotenv
```

### 3. Orquestación con Prefect
El archivo `ecommerce_pipeline.py` se encarga de disparar la sincronización en Airbyte mediante su API REST y ejecutar las transformaciones de dbt.

```bash
python ecommerce_pipeline.py
```

### 4. Despliegue de Metabase
Se utiliza un `Dockerfile` personalizado para incluir el driver de DuckDB necesario para conectar con MotherDuck.

```bash
docker build -f Dockerfile.metabase -t metabase-duckdb:latest .
docker run -d -p 3000:3000 --name metabase -v metabase-data:/metabase.db -e MB_PLUGINS_DIR=/plugins metabase-duckdb:latest
```

## 📈 Dashboard y Resultados
El panel final en Metabase responde preguntas clave de negocio, mostrando:
* KPIs Principales (Total Orders, Revenue, Margin, Average Order Value).
* Gráfico de tendencia de ingresos mensuales.
* Rendimiento y tasa de conversión por campaña de marketing (UTM).
* Análisis de ingresos por producto estrella.
* Serie temporal de ventas diarias.