**NBA Playoffs Data Pipeline**

Este proyecto implementa un proceso ETL completo donde la extracción de datos es el primer paso. A continuación se detalla específicamente cómo funciona este componente:

**Estructura de Repositorios**
Este proyecto opera con dos repositorios diferentes:

**1. Repositorio Fuente de Datos:** NBA-Data-2010-2024
- Contiene los datos originales de los playoffs de la NBA (play_off_totals_2010_2024.csv)


**2. Repositorio Actual (Proyecto ETL): Proyecto-ETL**

- Contiene todo el código de procesamiento ETL
- Incluye el script de extracción que copia datos desde el repositorio fuente al área de staging

**Proceso de Extracción y Staging**

El archivo test_extraction.py es el que conecta ambos repositorios:

**1. Extracción de datos:** El script se conecta al repositorio fuente mediante: 

**URL del repositorio fuente de datos de NBA**
repo_url = "https://github.com/NocturneBear/NBA-Data-2010-2024"

**2 Creación del área de staging:** El script crea una estructura de directorios dentro de este proyecto:

staging/

└── extract_20250309_120532/      # Directorio con timestamp de extracción

    ├── play_off_totals_2010_2024.csv  # Datos extraídos
    
    ├── _CONTROL.txt               # Archivo de control con metadatos
    
    └── _VALIDATION.json           # Resultados de validación
    
**3. Preparación para ETL:** Después de la extracción, los datos en el área de staging quedan listos para ser procesados por el componente de transformación.
