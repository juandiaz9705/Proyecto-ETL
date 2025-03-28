**NBA Playoffs ETL Analytics Dashboard (2010-2024)**

Este proyecto implementa un proceso ETL (Extract, Transform, Load) completo para analizar los datos de los playoffs de la NBA desde 2010 hasta 2024. El sistema automatiza la ingesta, procesamiento y análisis de datos de partidos de playoffs, generando métricas avanzadas y resúmenes por equipo y temporada.

**Tecnologías Utilizadas**
**Lenguajes de Programación**

- Python: Lenguaje principal para todos los componentes del sistema ETL

**Bibliotecas y Frameworks de Python**

- **Pandas:** Manipulación y análisis de datos
- **NumPy:** Soporte para operaciones numéricas
- **Schedule:** Programación de tareas automáticas
- **Logging:** Sistema de registro de actividades
- **Psycopg2:** Conexión con PostgreSQL
- **SQLAlchemy:** ORM para interacción con bases de datos
- **Tkinter:** Interfaz gráfica de usuario
- **PIL/Pillow:** Procesamiento de imágenes para la interfaz
- **Subprocess:** Ejecución de procesos externos
- **Threading:** Soporte para multihilos
- **Pathlib:** Manipulación de rutas de archivos

**Almacenamiento de Datos**

- **PostgreSQL:** Base de datos relacional para almacenar datos procesados
- **CSV:** Formato de almacenamiento de datos intermedio y final

**Control de Versiones**

- **Git:** Control de versiones para extracción de datos y desarrollo

**Integración y Automatización**

- **Programación por tareas:** Mediante la biblioteca Schedule
- **Sistema de reintentos:** Implementación personalizada para manejo de fallos
- **Archivos de control:** Para seguimiento y auditoría del proceso ETL


**Interfaz de Usuario**

- **Tkinter:** Framework para la interfaz gráfica
- **Canvas:** Para indicadores visuales de estado
- **ScrolledText:** Para visualización de logs

**Requisitos del Sistema**

- Python 3.8 o superior
- PostgreSQL 12 o superior
- Bibliotecas Python: pandas, numpy, psycopg2, sqlalchemy, PIL (Pillow), schedule, y otras dependencias



**Instalación**


**1. Clonar el Repositorio**

git clone https://github.com/juandiaz9705/Proyecto-ETL.git
cd Proyecto-ETL


**2. Crear Entorno Virtual (Opcional pero Recomendado)**

**Crear entorno virtual**

python -m venv venv

**Activar entorno virtual**
**En Windows:**

venv\Scripts\activate


**En macOS/Linux:**

source venv/bin/activate


**3. Instalar Dependencias**

pip install -r requirements.txt

pip install pandas numpy psycopg2-binary sqlalchemy pillow schedule


A continuación se muestra el contenido recomendado para el archivo requirements.txt:

- pandas==1.5.3
- numpy==1.24.3
- psycopg2-binary==2.9.6
- sqlalchemy==2.0.15
- pillow==9.5.0
-schedule==1.2.0


**4. Configurar Base de Datos PostgreSQL**

1. Asegúrate de tener PostgreSQL instalado y en funcionamiento.
2. Crear una base de datos para el proyecto:


CREATE DATABASE nba_playoffs;


**3. Configurar los parámetros de conexión en los archivos de configuración según sea necesario (por defecto se utiliza):**

- Host: localhost
- Puerto: 5432
- Base de datos: nba_playoffs
- Usuario: postgres
- Contraseña: 123

**5. Crear Estructura de Directorios**

El sistema requiere ciertos directorios para funcionar correctamente. Puedes crearlos manualmente o dejar que el sistema los cree automáticamente durante la primera ejecución:

mkdir -p data/staging data/processed_data logs processed_data/scripts image


**6. Copiar Archivos de Scripts**

Hay que asegurar de que los scripts principales están en la ubicación correcta:

cp *.py processed_data/scripts/


#Estructura del Proyecto

Proyecto-ETL/

├── automaticetl.py                  # Script de automatización principal

├── nba_etl.py                       # Componente de transformación y carga

├── test_extraction.py               # Componente de extracción

├── simplified_transformer.py        # Transformación avanzada

├── interface_app.py                 # Interfaz gráfica básica

├── interface_app2.py                # Interfaz gráfica mejorada

├── data/

│   ├── staging/                     # Área de staging para datos extraídos

│   │   └── extract_[timestamp]/

│   │       ├── play_off_totals_2010_2024.csv

│   │       ├── _CONTROL.txt

│   │       └── _VALIDATION.json

│   └── processed_data/              # Datos procesados y transformados

├── logs/                            # Directorio para archivos de registro

├── processed_data/

│   └── scripts/                     # Copias de los scripts principales para ejecución desde la interfaz

└── image/                           # Imágenes para la interfaz

    └── nba.png                      # Logo de la NBA para la interfaz



Los scripts principales pueden ubicarse tanto en el directorio raíz como en processed_data/scripts/ para permitir la ejecución tanto directa como desde la interfaz gráfica.


# Uso del Sistema

**1. Modo Interfaz Gráfica**

Para iniciar la interfaz gráfica mejorada, ejecuta:

python interface_app2.py


Desde la interfaz gráfica podrás:

- Ejecutar el proceso ETL completo: Automatiza todas las etapas en secuencia
- Ejecutar cada etapa individualmente:

   - Extracción (test_extraction.py)
   - Transformación/Carga básica (nba_etl.py)
   - Transformación avanzada (simplified_transformer.py)
   - Automatización (automaticetl.py)


**Operaciones de Base de Datos:**

- Limpiar Base de Datos
- Verificar Estado de BD


**Operaciones de Consola:**

- Limpiar Consola
- Mostrar Logs



**La interfaz proporciona indicadores visuales de estado para cada etapa:**

- Amarillo: Proceso en ejecución
- Verde: Proceso completado con éxito
- Rojo: Error en el proceso



# Arquitectura de Datos

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

- 1Verifica la estructura y contenido de los archivos CSV
- Genera estadísticas de validación en _VALIDATION.json



**3. Proceso de Transformación y Carga**

El componente de transformación y carga (nba_etl.py) implementa:


**Transformación de datos:**

- Limpieza y normalización de datos
- Cálculo de métricas avanzadas:

1. Eficiencia ofensiva
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

- El script automaticetl.py orquesta todo el proceso:

- Programación: Ejecución diaria automática (configurable)
- Reintentos: Sistema de reintentos con delay en caso de fallos
- Monitoreo: Logging detallado de todo el proceso
- Control: Archivos de control para verificación de ejecución


**5. Modelo de Datos**
El sistema utiliza las siguientes tablas en PostgreSQL:

1. nba_playoffs_detailed:

- Datos detallados de cada juego con métricas avanzadas


2. nba_playoffs_season_summary:

- Resumen de estadísticas por temporada


3. nba_playoffs_team_summary:

- Resumen de estadísticas por equipo


4. nba_playoffs_advanced:

- Almacena los datos de playoffs de la NBA con métricas avanzadas calculadas



#Licencia

Este proyecto se distribuye bajo la licencia MIT del uso del DataSet.

#Autor

Juan Diego Díaz Guzmán

#Agradecimientos
Los datos utilizados en este proyecto provienen del repositorio público NBA-Data-2010-2024, mantenido bajo licencia MIT.



