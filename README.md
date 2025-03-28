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
