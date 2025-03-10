**NBA Playoffs Data Pipeline**

Estructura de Repositorios
Este proyecto opera con dos repositorios diferentes:

1. Repositorio Fuente de Datos: NBA-Data-2010-2024

- Contiene los datos originales de los playoffs de la NBA (play_off_totals_2010_2024.csv)
- Es un repositorio externo que NO modificamos, solo consumimos sus datos


2. Repositorio Actual (Proyecto ETL): Proyecto-ETL

- Contiene todo nuestro código de procesamiento ETL
- Incluye el script de extracción que copia datos desde el repositorio fuente al área de staging



**Proceso de Extracción y Staging**

El archivo test_extraction.py es el que conecta ambos repositorios:

**1. Extracción de datos:** El script se conecta al repositorio fuente mediante:

URL del repositorio fuente de datos de NBA:
repo_url = "https://github.com/NocturneBear/NBA-Data-2010-2024"

**2. Creación del área de staging:** El script crea una estructura de directorios dentro de este proyecto:
Copystaging/
└── extract_20250309_120532/      # Directorio con timestamp de extracción
    ├── play_off_totals_2010_2024.csv  # Datos extraídos
    ├── _CONTROL.txt               # Archivo de control con metadatos
    └── _VALIDATION.json           # Resultados de validación

**3. Transformación de datos:**  Después de la extracción, los datos en el área de staging son procesados por el archivo nba_etl.py que:

Lee los datos del área de staging
Aplica transformaciones
Genera los archivos procesados en processed_data/



**Demostración del Flujo Completo**
Para ver el proceso completo realizar el siguiente procedimiento

**1. Ejecute la extracción:**
bashCopypython repository_to_staging.py --repo https://github.com/NocturneBear/NBA-Data-2010-2024
Este comando:

- Descarga los datos desde el repositorio fuente
- Crea un directorio en el área de staging con timestamp
- Copia y valida los datos extraídos
- Genera archivos de control


**2. Verifique el área de staging:**
Después de ejecutar el comando, el directorio staging/ contendrá una nueva carpeta con los datos extraídos y archivos de control.


**3. Continúe con el proceso ETL:**

python nba_etl.py
Este script automáticamente detecta y utiliza los datos más recientes del área de staging.


