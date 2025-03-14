**NBA Playoffs Data Pipeline**

Este proyecto implementa un proceso ETL(Extract, Transform, Load) completo para analizar los datos de los playoffs de la NBA desde 2010 hasta 2024. El sistema automatiza la ingesta, procesamiento y análisis de datos de partidos de playoffs, generando métricas avanzadas y resúmenes por equipo y temporada.

**Estructura del Proyecto**
El proyecto ETL está divido en tres componentes principales:


├── automaticetl.py        # Script de automatización principal

├── nba_etl.py             # Componente de transformación y carga 

├── test_extraction.py     # Componente de extracción 

├── logs/                  # Directorio para archivos de registro

├── processed_data/        # Datos procesados y transformados

└── staging/               # Área de staging para datos extraídos

    └── extract_[timestamp]/
    
        ├── play_off_totals_2010_2024.csv
        
        ├── _CONTROL.txt
        
        └── _VALIDATION.json


**Arquitectura de Datos**
**1. Fuente de Datos**

El proyecto obtiene datos del repositorio público:

- Repositorio: NBA-Data-2010-2024
- Archivo principal: play_off_totals_2010_2024.csv
- Características: 2,362 registros de juegos, 57 columnas de métricas
  

**2. Proceso de Extracción**

El componente de extracción (test_extraction.py) implementa el siguiente flujo:

**Conexión al repositorio fuente:**

- Descarga directa del archivo CSV mediante HTTPS
- Fallback a clonación completa del repositorio en caso necesario


**Creación del área de staging:**

- Genera un directorio con timestamp: staging/extract_[timestamp]/
- Copia los archivos extraídos
- Crea archivos de control y validación


**Validación de datos:**

- Verifica la estructura y contenido de los archivos CSV
- Genera estadísticas de validación en _VALIDATION.json



**3. Proceso de Transformación y Carga**

- El componente de transformación y carga (nba_etl.py) implementa:

**Transformación de datos:**

- Limpieza y normalización de datos
- Cálculo de métricas avanzadas:
  1.Eficiencia ofensiva
  2. Rating defensivo
  3. Plus/minus por minuto
  4. Ratio asistencias/pérdidas


**Generación de resúmenes:**

- Resúmenes por temporada
- Resúmenes por equipo
- Análisis de rendimiento


**Carga de datos:**

- Almacenamiento en PostgreSQL
- Generación de archivos CSV procesados
- Exportación para visualización



**4. Automatización del Pipeline**

El script automaticetl.py orquesta todo el proceso:

- Programación: Ejecución diaria automática (configurable)
- Reintentos: Sistema de reintentos con delay en caso de fallos
- Monitoreo: Logging detallado de todo el proceso
- Control: Archivos de control para verificación de ejecución

**Modelo de Datos**

- El sistema utiliza las siguientes tablas en PostgreSQL:

**1. nba_playoffs_detailed:**

- Datos detallados de cada juego con métricas avanzadas


**2. nba_playoffs_season_summary:**

- Resumen de estadísticas por temporada


**3. nba_playoffs_team_summary:**

- Resumen de estadísticas por equipo

**4. nba_playoffs_advanced:**
 - Almacena los datos de playoffs de la NBA con métricas avanzadas calculadas.
