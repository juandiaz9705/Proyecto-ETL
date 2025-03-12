import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values
import sys
from pathlib import Path


class NBAPlayoffsETL:

    
    def __init__(self, input_file=None, db_config=None, staging_dir='data/staging'):

        self.staging_dir = Path(staging_dir)
        self.input_file = input_file or self._find_latest_input_file()
        self.raw_data = None
        self.transformed_data = None
        
        # Configuración predeterminada de la base de datos si no se proporciona
        if db_config is None:
            self.db_config = {
                'host': 'localhost',
                'port': '5432',
                'database': 'nba_playoffs',
                'user': 'postgres',
                'password': '123'
            }
        else:
            self.db_config = db_config
            
        self.engine = None
        self.conn = None

        # Configurar logging
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/etl_process_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Crear directorio para datos procesados si no existe
        processed_dir = Path('data/processed_data')
        processed_dir.mkdir(exist_ok=True, parents=True)
        
        # Intentar establecer conexión con la base de datos
        try:
            self._create_db_connection()
        except Exception as e:
            self.logger.warning(f"No se pudo conectar a la base de datos: {str(e)}")
            self.logger.info("Continuando en modo sin conexión a base de datos")

    def _find_latest_input_file(self):
     
        try:
            # Buscar el directorio más reciente en el área de staging
            staging_dirs = [d for d in self.staging_dir.glob('extract_*') if d.is_dir()]
            if not staging_dirs:
                # Si no hay directorios en staging, buscar el archivo directamente en el directorio raíz
                default_file = Path('data/play_off_totals_2010_2024.csv')
                if default_file.exists():
                    return str(default_file)
                raise FileNotFoundError("No se encontraron directorios de extracción en staging ni archivo predeterminado")
                
            latest_dir = max(staging_dirs, key=lambda d: d.name)
            
            # Buscar archivos CSV en el directorio más reciente
            csv_files = list(latest_dir.glob('*.csv'))
            if not csv_files:
                raise FileNotFoundError(f"No se encontraron archivos CSV en {latest_dir}")
                
            # Priorizar archivos que contengan 'play_off' en el nombre
            playoff_files = [f for f in csv_files if 'play_off' in f.name.lower()]
            if playoff_files:
                return str(playoff_files[0])
            
            # Si no hay archivos con 'play_off', tomar el primer CSV
            return str(csv_files[0])
            
        except Exception as e:
            self.logger.error(f"Error al buscar archivo de entrada: {str(e)}")
            default_file = 'play_off_totals_2010_2024.csv'
            self.logger.info(f"Usando archivo predeterminado: {default_file}")
            return default_file

    def _create_db_connection(self):
     
        try:
            # Conexión SQLAlchemy para pandas
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)

            # Conexión psycopg2 para inserciones masivas
            self.conn = psycopg2.connect(
                dbname=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )

            self.logger.info("Conexión a base de datos establecida correctamente")
        except Exception as e:
            self.logger.error(f"Error de conexión a base de datos: {str(e)}")
            raise

    def _create_tables(self):
       
        try:
            tables = [
                """
                DROP TABLE IF EXISTS nba_playoffs_detailed;
                CREATE TABLE nba_playoffs_detailed (
                    id SERIAL PRIMARY KEY,
                    season_year VARCHAR(10),
                    team_id INTEGER,
                    team_name VARCHAR(100),
                    game_date DATE,
                    matchup VARCHAR(50),
                    wl CHAR(1),
                    pts INTEGER,
                    fg3m INTEGER,
                    ast INTEGER,
                    offensive_efficiency FLOAT,
                    defensive_rating FLOAT,
                    plus_minus_per_min FLOAT,
                    ast_to_ratio FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                DROP TABLE IF EXISTS nba_playoffs_season_summary;
                CREATE TABLE nba_playoffs_season_summary (
                    season_year VARCHAR(10) PRIMARY KEY,
                    avg_pts FLOAT,
                    avg_fg3m FLOAT,
                    avg_ast FLOAT,
                    avg_off_efficiency FLOAT,
                    avg_def_rating FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                DROP TABLE IF EXISTS nba_playoffs_team_summary;
                CREATE TABLE nba_playoffs_team_summary (
                    team_name VARCHAR(100) PRIMARY KEY,
                    avg_pts FLOAT,
                    win_rate FLOAT,
                    avg_off_efficiency FLOAT,
                    avg_def_rating FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            ]

            with self.conn.cursor() as cur:
                for table_sql in tables:
                    cur.execute(table_sql)
                self.conn.commit()

            self.logger.info("Tablas creadas correctamente")
            return True

        except Exception as e:
            self.logger.error(f"Error al crear tablas: {str(e)}")
            return False

    def extract(self):
       
        try:
            self.logger.info(f"Iniciando extracción de datos desde {self.input_file}")
            self.raw_data = pd.read_csv(self.input_file)
            self.logger.info(f"Datos extraídos correctamente. Forma: {self.raw_data.shape}")

            required_columns = ['SEASON_YEAR', 'TEAM_NAME', 'GAME_DATE', 'PTS', 'FG3M', 'AST', 'WL']
            missing_columns = [col for col in required_columns if col not in self.raw_data.columns]

            if missing_columns:
                raise ValueError(f"Columnas faltantes en el conjunto de datos: {missing_columns}")

            # Guardar información de verificación de datos
            stats = {
                'total_rows': len(self.raw_data),
                'columns': len(self.raw_data.columns),
                'seasons': self.raw_data['SEASON_YEAR'].nunique(),
                'teams': self.raw_data['TEAM_NAME'].nunique(),
                'extraction_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Guardar estadísticas en archivo de logs
            with open(f'logs/extraction_stats_{datetime.now().strftime("%Y%m%d")}.txt', 'w') as f:
                for key, value in stats.items():
                    f.write(f"{key}: {value}\n")
            
            return True

        except Exception as e:
            self.logger.error(f"Error en la extracción de datos: {str(e)}")
            return False

    def transform(self):
      
        try:
            self.logger.info("Iniciando transformación de datos")
            df = self.raw_data.copy()

            # Convertir fechas
            df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

            # Manejar valores nulos
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(0)
            
            # Registrar información sobre valores nulos
            null_counts = df.isnull().sum()
            columns_with_nulls = null_counts[null_counts > 0]
            if not columns_with_nulls.empty:
                self.logger.info("Valores nulos por columna:")
                for col, count in columns_with_nulls.items():
                    self.logger.info(f"  - {col}: {count}")

            # Calcular métricas avanzadas con manejo de errores
            try:
                # Eficiencia ofensiva: puntos por posesión estimada
                df['OFFENSIVE_EFFICIENCY'] = df.apply(
                    lambda row: row['PTS'] / max(1, (row['FGA'] - row.get('OREB', 0) + row.get('TOV', 0))),
                    axis=1
                ).round(3)
                
                # Rating defensivo: (robos + tapones) / pérdidas
                df['DEFENSIVE_RATING'] = df.apply(
                    lambda row: (row.get('STL', 0) + row.get('BLK', 0)) / max(1, row.get('TOV', 1)),
                    axis=1
                ).round(3)
                
                # Plus/minus por minuto
                df['PLUS_MINUS_PER_MIN'] = df.apply(
                    lambda row: row.get('PLUS_MINUS', 0) / max(1, row.get('MIN', 30)),
                    axis=1
                ).round(3)
                
                # Ratio asistencias/pérdidas
                df['AST_TO_RATIO'] = df.apply(
                    lambda row: row['AST'] / max(1, row.get('TOV', 1)),
                    axis=1
                ).round(3)
            
            except Exception as e:
                self.logger.warning(f"Error al calcular algunas métricas: {str(e)}")
                self.logger.info("Usando cálculos alternativos para métricas")
                
                # Cálculos alternativos más seguros
                df['OFFENSIVE_EFFICIENCY'] = (df['PTS'] / df['FGA'].clip(lower=1)).round(3)
                df['DEFENSIVE_RATING'] = 0.0  # Valor predeterminado
                df['PLUS_MINUS_PER_MIN'] = 0.0  # Valor predeterminado
                df['AST_TO_RATIO'] = (df['AST'] / 1).round(3)  # Valor predeterminado

            # Resumen por temporada
            self.season_summary = df.groupby('SEASON_YEAR').agg({
                'PTS': 'mean',
                'FG3M': 'mean',
                'AST': 'mean',
                'OFFENSIVE_EFFICIENCY': 'mean',
                'DEFENSIVE_RATING': 'mean'
            }).round(2)

            # Resumen por equipo
            self.team_summary = df.groupby('TEAM_NAME').agg({
                'PTS': 'mean',
                'WL': lambda x: (x == 'W').mean(),
                'OFFENSIVE_EFFICIENCY': 'mean',
                'DEFENSIVE_RATING': 'mean'
            }).round(2)

            self.transformed_data = df
            
            # Guardar datos transformados en formato CSV para uso por la aplicación Flask
            output_path = Path('data/processed_data') / f'playoffs_detailed_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_path, index=False)
            self.logger.info(f"Datos transformados guardados en: {output_path}")
            
            # También guardar una copia con nombre constante para la aplicación Flask
            df.to_csv(Path('data/processed_data') / 'playoffs_detailed.csv', index=False)
            
            self.logger.info("Transformación completada correctamente")
            return True

        except Exception as e:
            self.logger.error(f"Error en la transformación: {str(e)}")
            return False

    def load(self):
        
        try:
            self.logger.info("Iniciando carga de datos a PostgreSQL")

            # Verificar si tenemos conexión a la base de datos
            if not self.conn or not self.engine:
                self.logger.warning("No hay conexión a la base de datos. Saltando paso de carga.")
                self.logger.info("Los datos transformados se han guardado en archivos CSV para uso offline.")
                return False

            self._create_tables()

            # Preparar datos detallados
            detailed_data = self.transformed_data[[
                'SEASON_YEAR', 'TEAM_ID', 'TEAM_NAME', 'GAME_DATE', 'MATCHUP', 'WL',
                'PTS', 'FG3M', 'AST', 'OFFENSIVE_EFFICIENCY', 'DEFENSIVE_RATING',
                'PLUS_MINUS_PER_MIN', 'AST_TO_RATIO'
            ]].copy()

            # Convertir a lista de tuplas para psycopg2 - Convertir tipos NumPy a Python
            detailed_records = [tuple(x.item() if hasattr(x, 'item') else x for x in row) 
                           for row in detailed_data.values]

            with self.conn.cursor() as cur:
                # Insertar datos detallados usando execute_values
                execute_values(
                    cur,
                    """
                    INSERT INTO nba_playoffs_detailed 
                    (season_year, team_id, team_name, game_date, matchup, wl, 
                     pts, fg3m, ast, offensive_efficiency, defensive_rating,
                     plus_minus_per_min, ast_to_ratio)
                    VALUES %s
                    """,
                    detailed_records,
                    page_size=100
                )

                # Convertir objetos NumPy a tipos Python estándar para el resumen por temporada
                season_records = [
                    (index, 
                     float(row['PTS']), 
                     float(row['FG3M']), 
                     float(row['AST']),
                     float(row['OFFENSIVE_EFFICIENCY']), 
                     float(row['DEFENSIVE_RATING']))
                    for index, row in self.season_summary.iterrows()
                ]

                # Insertar resumen por temporada
                execute_values(
                    cur,
                    """
                    INSERT INTO nba_playoffs_season_summary 
                    (season_year, avg_pts, avg_fg3m, avg_ast, 
                     avg_off_efficiency, avg_def_rating)
                    VALUES %s
                    """,
                    season_records,
                    page_size=100
                )

                # Convertir objetos NumPy a tipos Python estándar para el resumen por equipo
                team_records = [
                    (index, 
                     float(row['PTS']), 
                     float(row['WL']),
                     float(row['OFFENSIVE_EFFICIENCY']), 
                     float(row['DEFENSIVE_RATING']))
                    for index, row in self.team_summary.iterrows()
                ]

                # Insertar resumen por equipo
                execute_values(
                    cur,
                    """
                    INSERT INTO nba_playoffs_team_summary 
                    (team_name, avg_pts, win_rate, 
                     avg_off_efficiency, avg_def_rating)
                    VALUES %s
                    """,
                    team_records,
                    page_size=100
                )

                self.conn.commit()

            self.logger.info("Datos cargados correctamente a PostgreSQL")
            return True

        except Exception as e:
            self.logger.error(f"Error en la carga de datos: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.conn.close()
                self.logger.info("Conexión a base de datos cerrada")

    def run_pipeline(self):
   
        self.logger.info("Iniciando pipeline ETL")

        if not self.extract():
            self.logger.error("Extracción fallida. Deteniendo pipeline.")
            return False

        if not self.transform():
            self.logger.error("Transformación fallida. Deteniendo pipeline.")
            return False

        if not self.load():
            self.logger.warning("Carga a base de datos fallida. Los datos están disponibles en archivos CSV.")
            # No consideramos esto un error crítico si tenemos los datos en CSV
            
        # Crear archivo de control para indicar ejecución exitosa
        control_file = Path('logs') / f'etl_success_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(control_file, 'w') as f:
            f.write(f"ETL ejecutado correctamente: {datetime.now().isoformat()}\n")
            f.write(f"Archivo de entrada: {self.input_file}\n")
            if self.raw_data is not None:
                f.write(f"Filas procesadas: {len(self.raw_data)}\n")

        self.logger.info("Pipeline ETL completado correctamente")
        return True


def main():
    """Función principal para ejecutar el ETL desde línea de comandos."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NBA Playoffs ETL Process')
    parser.add_argument('--input', type=str, help='Archivo de entrada CSV')
    parser.add_argument('--host', type=str, default='localhost', help='Host de base de datos')
    parser.add_argument('--port', type=str, default='5432', help='Puerto de base de datos')
    parser.add_argument('--db', type=str, default='nba_playoffs', help='Nombre de base de datos')
    parser.add_argument('--user', type=str, default='postgres', help='Usuario de base de datos')
    parser.add_argument('--password', type=str, default='123', help='Contraseña de base de datos')
    
    args = parser.parse_args()
    
    # Configuración de la base de datos
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.db,
        'user': args.user,
        'password': args.password
    }

    # Iniciar ETL
    etl = NBAPlayoffsETL(input_file=args.input, db_config=db_config)
    success = etl.run_pipeline()

    if success:
        print("\Proceso ETL completado correctamente\n")
        return 0
    else:
        print("\nError en el proceso ETL. Revise los logs para más detalles.\n")
        return 1


if __name__ == "__main__":
    exit(main())