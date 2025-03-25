"""
Autor: Juan Diego Díaz Guzmán
Fecha: 13-03-2025
Modificado para solucionar problema con creación de tablas
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import traceback
from pathlib import Path
import sys
import os
import argparse


# Intentar importar las librerías para PostgreSQL
try:
    import psycopg2
    from sqlalchemy import create_engine
    from psycopg2.extras import execute_values
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("Aviso: psycopg2 o sqlalchemy no están instalados. La carga en PostgreSQL no estará disponible.")

class NBAPlayoffsAdvancedTransformer:
    """Transformador simplificado para datos de playoffs NBA con soporte PostgreSQL"""
    
    def __init__(self, input_file=None, output_dir='processed_data', db_config=None):
        # Configurar logging primero
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/advanced_transform_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Configuración básica
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Configuración de base de datos
        self.db_config = db_config or {
            'host': 'localhost',
            'port': '5432',
            'database': 'nba_playoffs',
            'user': 'postgres',
            'password': '123'
        }
        
        # Inicializar conexiones a None
        self.engine = None
        self.conn = None
        
        # Crear conexión a la base de datos si está disponible
        if DATABASE_AVAILABLE and db_config is not None:
            self._create_db_connection()
        
        # Cargar datos
        try:
            if input_file is None:
                input_file = self._find_input_file()
            
            self.logger.info(f"Cargando datos desde {input_file}")
            self.data = pd.read_csv(input_file)
            self.logger.info(f"Datos cargados: {len(self.data)} registros, {len(self.data.columns)} columnas")
        except Exception as e:
            self.logger.error(f"Error al cargar datos: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
    
    def _create_db_connection(self):
        """Crea conexiones a PostgreSQL"""
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
            self.logger.error(traceback.format_exc())
            self.engine = None
            self.conn = None
            print(f"Error al conectar a PostgreSQL: {e}")
    
    def _verify_tables(self):
        """Verifica que las tablas existan y las crea si es necesario"""
        if not DATABASE_AVAILABLE or not self.conn:
            self.logger.warning("No hay conexión a base de datos. No se pueden verificar tablas.")
            return False
            
        try:
            with self.conn.cursor() as cur:
                # Verificar tablas requeridas
                tables_to_check = [
                    'nba_playoffs_advanced',
                    'nba_playoffs_team_summary',
                    'nba_playoffs_season_summary'
                ]
                
                missing_tables = []
                for table in tables_to_check:
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = '{table}'
                        );
                    """)

                    exists = cur.fetchone()[0]
                    
                    if not exists:
                        missing_tables.append(table)

                if missing_tables:
                    self.logger.warning(f"Tablas faltantes: {missing_tables}")
                    print(f"Creando tablas faltantes: {missing_tables}")

                    # Crear tablas faltantes
                    if 'nba_playoffs_advanced' in missing_tables:
                        cur.execute("""
                        CREATE TABLE nba_playoffs_advanced (
                            id SERIAL PRIMARY KEY,
                            season_year VARCHAR(10),
                            team_id INTEGER,
                            team_name VARCHAR(100),
                            game_date DATE,
                            matchup VARCHAR(50),
                            wl CHAR(1),
                            pts INTEGER,
                            ast INTEGER,
                            fg3m INTEGER,
                            offensive_efficiency FLOAT,
                            defensive_rating FLOAT,
                            ast_to_ratio FLOAT,
                            playoff_efficiency FLOAT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        """)
                    if 'nba_playoffs_team_summary' in missing_tables:
                        cur.execute("""
                        CREATE TABLE nba_playoffs_team_summary (
                            team_name VARCHAR(100) PRIMARY KEY,
                            pts FLOAT,
                            win_rate FLOAT,
                            ast FLOAT,
                            offensive_efficiency FLOAT,
                            defensive_rating FLOAT,
                            playoff_efficiency FLOAT, 
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        """)
                    if 'nba_playoffs_season_summary' in missing_tables:
                        cur.execute("""
                        CREATE TABLE nba_playoffs_season_summary (
                            season_year VARCHAR(10) PRIMARY KEY,
                            avg_pts FLOAT,
                            avg_fg3m FLOAT,
                            avg_ast FLOAT,
                            avg_off_efficiency FLOAT,
                            avg_def_rating FLOAT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        """)
                    self.conn.commit()
                    self.logger.info("Tablas faltantes creadas correctamente")
                
                return True
            
        except Exception as e:
            self.logger.error(f"Error al verificar tablas: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    def _create_tables(self):
        """Crea las tablas necesarias en PostgreSQL"""
        if not DATABASE_AVAILABLE or not self.conn:
            self.logger.warning("No hay conexión a base de datos. No se pueden crear tablas.")
            return False    
        
        try:
            # Primero verificar si las tablas ya existen
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'nba_playoffs_advanced'
                    );
                """)

                exists = cur.fetchone()[0]
                
                if exists:
                    self.logger.info("Las tablas ya existen. No es necesario crearlas.")
                    return True
                
            # Las tablas no existen, proceder a crearlas
            tables = [
            """
            DROP TABLE IF EXISTS nba_playoffs_advanced;
            CREATE TABLE nba_playoffs_advanced (
                id SERIAL PRIMARY KEY,
                season_year VARCHAR(10),
                team_id INTEGER,
                team_name VARCHAR(100),
                game_date DATE,
                matchup VARCHAR(50),
                wl CHAR(1),
                pts INTEGER,
                ast INTEGER,
                fg3m INTEGER,
                offensive_efficiency FLOAT,
                defensive_rating FLOAT,
                ast_to_ratio FLOAT,
                playoff_efficiency FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            DROP TABLE IF EXISTS nba_playoffs_team_summary;
            CREATE TABLE nba_playoffs_team_summary (
                team_name VARCHAR(100) PRIMARY KEY,
                pts FLOAT,
                win_rate FLOAT,
                ast FLOAT,
                offensive_efficiency FLOAT,
                defensive_rating FLOAT,
                playoff_efficiency FLOAT, 
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
            self.logger.error(traceback.format_exc())
            if self.conn:
                self.conn.rollback()
            return False
        
    def _find_input_file(self):
        """Busca un archivo de datos adecuado para procesar"""
        possible_files = [
            'play_off_totals_2010_2024.csv',
            'playoffs_detailed.csv',
            'playoffs_detailed_processed.csv',
            Path('processed_data') / 'playoffs_detailed.csv'
        ]

    # Buscar en el directorio de staging también
        staging_dir = Path('staging')
        if staging_dir.exists():
            extract_dirs = [d for d in staging_dir.glob('extract_*') if d.is_dir()]
            if extract_dirs:
                latest_dir = max(extract_dirs, key=lambda d: d.name)
                for file in latest_dir.glob('*.csv'):
                    if 'play_off' in file.name.lower():
                        possible_files.append(file)

        # Verificar cada archivo posible
        for file_path in possible_files:
            path = Path(file_path)
            if path.exists():
                self.logger.info(f"Archivo encontrado: {path}")
                return str(path)
            

         # Si no se encuentra, buscar en cualquier subdirectorio
        self.logger.warning("No se encontraron archivos en las ubicaciones estándar. Buscando en todo el directorio...")
        
        # Buscar recursivamente en todos los directorios
        csv_files = list(Path('.').glob('**/*.csv'))
        if csv_files:
            for file in csv_files:
                if 'play_off' in file.name.lower():
                    self.logger.info(f"Archivo encontrado en ubicación alternativa: {file}")
                    return str(file)
            
            # Si no hay archivos con 'play_off' en el nombre, usar el primero que se encuentre
            self.logger.info(f"Usando primer archivo CSV encontrado: {csv_files[0]}")
            return str(csv_files[0])
        raise FileNotFoundError("No se encontró ningún archivo de datos para procesar")


    def preprocess_data(self):
        """Preprocesa los datos para el análisis"""
        self.logger.info("Iniciando preprocesamiento de datos")
        
        df = self.data.copy()
        
        # Convertir fechas
        try:
            df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
        except Exception as e:
            self.logger.warning(f"Error al convertir fechas: {str(e)}")
        
        # Manejar valores nulos
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna(0)
        
        self.preprocessed_data = df
        self.logger.info("Preprocesamiento completado")
        return df

    def calculate_advanced_metrics(self):
        """Calcula métricas avanzadas de rendimiento"""
        self.logger.info("Calculando métricas avanzadas")
        
        if not hasattr(self, 'preprocessed_data'):
            df = self.preprocess_data()
        else:
            df = self.preprocessed_data.copy()
        
        try:
            # Eficiencia ofensiva
            if all(col in df.columns for col in ['PTS', 'FGA']):
                df['OFFENSIVE_EFFICIENCY'] = (df['PTS'] / df['FGA'].clip(lower=1)).round(3)
            
            # Rating defensivo
            if all(col in df.columns for col in ['STL', 'BLK', 'TOV']):
                df['DEFENSIVE_RATING'] = ((df['STL'] + df['BLK']) / df['TOV'].clip(lower=1)).round(3)
            
            # Ratio asistencias/pérdidas
            if all(col in df.columns for col in ['AST', 'TOV']):
                df['AST_TO_RATIO'] = (df['AST'] / df['TOV'].clip(lower=1)).round(3)
            
            # Eficiencia en los playoffs (métrica personalizada)
            if all(col in df.columns for col in ['PTS', 'REB', 'AST', 'STL', 'BLK', 'FGM', 'FGA', 'FTM', 'FTA', 'TOV']):
                df['PLAYOFF_EFFICIENCY'] = (
                    (df['PTS'] + df['REB'] + df['AST'] + df['STL'] + df['BLK']) - 
                    ((df['FGA'] - df['FGM']) + (df['FTA'] - df['FTM']) + df['TOV'])
                ).round(2)
            
            self.logger.info("Métricas avanzadas calculadas correctamente")

        except Exception as e:
            self.logger.error(f"Error al calcular métricas avanzadas: {str(e)}")
            self.logger.error(traceback.format_exc())
        
        self.advanced_metrics = df
        return df

    def create_summaries(self):
        """Crea resúmenes por equipo y temporada"""
        self.logger.info("Generando resúmenes")
        
        if not hasattr(self, 'advanced_metrics'):
            df = self.calculate_advanced_metrics()
        else:
            df = self.advanced_metrics.copy()
        
        try:
            # Resumen por equipo
            self.team_summary = df.groupby('TEAM_NAME').agg({
                'PTS': 'mean',
                'AST': 'mean',
                'REB': 'mean',
                'FG_PCT': 'mean',
                'FG3_PCT': 'mean',
                'WL': lambda x: (x == 'W').mean(),  # Tasa de victorias
                'OFFENSIVE_EFFICIENCY': 'mean',
                'PLAYOFF_EFFICIENCY': 'mean',
            }).round(3)




            # Renombrar columnas para coincidir con la BD
            self.team_summary.rename(columns={
                'WL': 'WIN_RATE',
                'PTS': 'AVG_PTS'  # Para coincidir con avg_pts en la BD
            }, inplace=True)
            
            # Resumen por temporada
            self.season_summary = df.groupby('SEASON_YEAR').agg({
                'PTS': 'mean',
                'AST': 'mean', 
                'FG3M': 'mean',
                'FG_PCT': 'mean',
                'OFFENSIVE_EFFICIENCY': 'mean',
                'DEFENSIVE_RATING': 'mean',
            }).round(3)

            self.logger.info("Resúmenes generados correctamente")
        
        except Exception as e:
            self.logger.error(f"Error al generar resúmenes: {str(e)}")
            self.logger.error(traceback.format_exc())

            # Generar resúmenes básicos si algo falla
            try:
                self.team_summary = df.groupby('TEAM_NAME').agg({'PTS': 'mean', 'WL': lambda x: (x == 'W').mean()}).round(3)
                self.team_summary.rename(columns={'WL': 'WIN_RATE'}, inplace=True)
                
                self.season_summary = df.groupby('SEASON_YEAR').agg({'PTS': 'mean', 'FG3M': 'mean'}).round(3)
                self.logger.info("Resúmenes básicos generados como alternativa")
            except:
                self.logger.error("No se pudieron generar resúmenes")
        
        return self.team_summary, self.season_summary

    def save_to_files(self):
        """Guarda los resultados del análisis en archivos CSV"""
        self.logger.info("Guardando resultados en archivos CSV")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Guardar datos con métricas avanzadas
            if hasattr(self, 'advanced_metrics'):
                advanced_file = self.output_dir / f'playoffs_advanced_{timestamp}.csv'
                self.advanced_metrics.to_csv(advanced_file, index=False)
                self.logger.info(f"Guardado: {advanced_file}")
                
                # Copia con nombre estándar
                self.advanced_metrics.to_csv(self.output_dir / 'playoffs_advanced.csv', index=False)
            
            # Guardar resúmenes
            if hasattr(self, 'team_summary'):
                team_file = self.output_dir / 'team_summary.csv'
                self.team_summary.to_csv(team_file)
                self.logger.info(f"Guardado: {team_file}")
            
            if hasattr(self, 'season_summary'):
                season_file = self.output_dir / 'season_summary.csv'
                self.season_summary.to_csv(season_file)
                self.logger.info(f"Guardado: {season_file}")

            # Crear archivo de control
            control_file = Path('logs') / f'advanced_transform_success_{timestamp}.txt'
            with open(control_file, 'w') as f:
                f.write(f"Transformación ejecutada: {datetime.now().isoformat()}\n")
                if hasattr(self, 'advanced_metrics'):
                    f.write(f"Registros procesados: {len(self.advanced_metrics)}\n")

            self.logger.info("Resultados guardados correctamente en archivos")
            return True
        
        except Exception as e:
            self.logger.error(f"Error al guardar resultados en archivos: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        
    def save_to_postgresql(self):
        """Guarda los resultados en PostgreSQL"""
        if not DATABASE_AVAILABLE or not self.conn:
            self.logger.warning("No hay conexión a PostgreSQL. No se pueden guardar datos.")
            return False
    
        # Primero verificar que las tablas existen
        if not self._verify_tables():
            if not self._create_tables():
                self.logger.error("No se pudieron crear las tablas necesarias.")
                return False
    
        self.logger.info("Guardando resultados en PostgreSQL")

        try:
            # Limpiar tablas antes de insertar nuevos datos
            with self.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE nba_playoffs_advanced RESTART IDENTITY CASCADE;")
                cur.execute("TRUNCATE TABLE nba_playoffs_team_summary RESTART IDENTITY CASCADE;")
                cur.execute("TRUNCATE TABLE nba_playoffs_season_summary RESTART IDENTITY CASCADE;")
                self.conn.commit()

            # Insertar datos avanzados
            if hasattr(self, 'advanced_metrics'):
                # Columnas a guardar del DataFrame de métricas avanzadas
                advanced_columns = ['SEASON_YEAR', 'TEAM_ID', 'TEAM_NAME', 'GAME_DATE', 'MATCHUP', 'WL', 
                        'PTS', 'AST', 'FG3M', 'OFFENSIVE_EFFICIENCY', 'DEFENSIVE_RATING', 
                        'AST_TO_RATIO', 'PLAYOFF_EFFICIENCY']
                
                # Verificar qué columnas existen realmente en el DataFrame
                available_advanced_columns = [col for col in advanced_columns if col in self.advanced_metrics.columns]

                #Seleccionar las columnas disponibles
                df_to_save = self.advanced_metrics[available_advanced_columns].copy()
            
                # Convertir fecha a formato adecuado para PostgreSQL
                if 'GAME_DATE' in df_to_save.columns and hasattr(df_to_save['GAME_DATE'], 'dt'):
                    df_to_save['GAME_DATE'] = df_to_save['GAME_DATE'].dt.strftime('%Y-%m-%d')

                # Convertir a lista de tuplas
                advanced_records = [tuple(x) for x in df_to_save.values]
                
                # Insertar datos avanzados
                with self.conn.cursor() as cur:
                    # Preparar formato SQL según columnas disponibles 
                    advanced_columns_sql = ', '.join([col.lower() for col in available_advanced_columns])
                    
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO nba_playoffs_advanced 
                        ({advanced_columns_sql})
                        VALUES %s
                        """,
                        advanced_records,
                        page_size=100
                    )
                
                    self.logger.info(f"Insertados {len(advanced_records)} registros en nba_playoffs_advanced")

            # Guardar resumen por equipo
            if hasattr(self, 'team_summary'):
                # Preparar datos
                team_data = self.team_summary.reset_index().copy()
                
                # MODIFICADO: Renombrar columnas para coincidir con la estructura de la tabla
                if 'PTS' in team_data.columns:
                    team_data.rename(columns={'PTS': 'AVG_PTS'}, inplace=True)
            
                # Verificamos las columnas que realmente existen en la tabla
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'nba_playoffs_team_summary'
                    """)
                    table_columns = [row[0] for row in cur.fetchall()]
                
                self.logger.info(f"Columnas en la tabla nba_playoffs_team_summary: {table_columns}")
            
                # Mapeo de nombres de columnas de DataFrame a BD
                team_column_mapping = {
                    'TEAM_NAME': 'team_name',
                    'AVG_PTS': 'avg_pts',
                    'WIN_RATE': 'win_rate',
                    'OFFENSIVE_EFFICIENCY': 'avg_off_efficiency',
                    'DEFENSIVE_RATING': 'avg_def_rating'
                }
            
                # Seleccionar solo las columnas que existen en el DataFrame y pueden mapearse a la tabla
                valid_df_columns = []
                valid_db_columns = []
            
                for df_col, db_col in team_column_mapping.items():
                    if df_col in team_data.columns and db_col in table_columns:
                        valid_df_columns.append(df_col)
                        valid_db_columns.append(db_col)
            
                # Seleccionar solo las columnas válidas
                if valid_df_columns:
                    team_data_filtered = team_data[valid_df_columns].copy()
                    
                    # Convertir a lista de tuplas
                    team_records = [tuple(x) for x in team_data_filtered.values]
                    
                    # Columnas para SQL
                    team_columns_sql = ', '.join(valid_db_columns)
                    
                    # Insertar datos
                    with self.conn.cursor() as cur:
                        execute_values(
                            cur,
                            f"""
                            INSERT INTO nba_playoffs_team_summary 
                            ({team_columns_sql})
                            VALUES %s
                            """,
                            team_records,
                            page_size=100
                        )
                        
                        self.logger.info(f"Insertados {len(team_records)} registros en nba_playoffs_team_summary")
                else:
                    self.logger.warning("No se encontraron columnas válidas para insertar en nba_playoffs_team_summary")

            # Guardar resumen por temporada - Aplicamos el mismo enfoque adaptativo
            if hasattr(self, 'season_summary'):
                # Preparar datos
                season_data = self.season_summary.reset_index().copy()
                
                # Verificamos las columnas que realmente existen en la tabla
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'nba_playoffs_season_summary'
                    """)
                    table_columns = [row[0] for row in cur.fetchall()]
                
                self.logger.info(f"Columnas en la tabla nba_playoffs_season_summary: {table_columns}")
            
            # Mapeo de nombres de columnas de DataFrame a BD
            season_column_mapping = {
                'SEASON_YEAR': 'season_year',
                'PTS': 'avg_pts',
                'FG3M': 'avg_fg3m',
                'AST': 'avg_ast',
                'OFFENSIVE_EFFICIENCY': 'avg_off_efficiency',
                'DEFENSIVE_RATING': 'avg_def_rating'
            }
            
            # Seleccionar solo las columnas que existen en el DataFrame y pueden mapearse a la tabla
            valid_df_columns = []
            valid_db_columns = []
            
            for df_col, db_col in season_column_mapping.items():
                if df_col in season_data.columns and db_col in table_columns:
                    valid_df_columns.append(df_col)
                    valid_db_columns.append(db_col)
            
            # Seleccionar solo las columnas válidas
            if valid_df_columns:
                season_data_filtered = season_data[valid_df_columns].copy()
                
                # Convertir a lista de tuplas
                season_records = [tuple(x) for x in season_data_filtered.values]
                
                # Columnas para SQL
                season_columns_sql = ', '.join(valid_db_columns)
                
                # Insertar datos
                with self.conn.cursor() as cur:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO nba_playoffs_season_summary 
                        ({season_columns_sql})
                        VALUES %s
                        """,
                        season_records,
                        page_size=100
                    )
                    
                    self.logger.info(f"Insertados {len(season_records)} registros en nba_playoffs_season_summary")
            else:
                    self.logger.warning("No se encontraron columnas válidas para insertar en nba_playoffs_season_summary")

            # Commit de todas las transacciones
            self.conn.commit()
            self.logger.info("Datos guardados correctamente en PostgreSQL")

            return True
        
        except Exception as e:
            self.logger.error(f"Error al guardar en PostgreSQL: {str(e)}")
            self.logger.error(traceback.format_exc())
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
            return False
        
    def run_pipeline(self):
        """Ejecuta el pipeline completo de transformación"""
        self.logger.info("Iniciando pipeline de transformación")
        
        try:

            # Ejecutar todas las etapas
            self.preprocess_data()
            self.calculate_advanced_metrics()
            self.create_summaries()
                
            # Guardar en archivos
            file_success = self.save_to_files()
            
            # Guardar en PostgreSQL si es posible
            db_success = False
            if DATABASE_AVAILABLE and self.db_config is not None:
                # Recrear la conexión si fue cerrada
                if not self.conn or (hasattr(self.conn, 'closed') and self.conn.closed):
                    self._create_db_connection()
                    
                # Crear tablas si no existen
                if self.conn:
                    self._verify_tables()

                # Guardar datos
                db_success = self.save_to_postgresql()
                
                if db_success:
                    print("Datos guardados exitosamente en PostgreSQL")
                else:
                    print("Advertencia: No se pudieron guardar los datos en PostgreSQL")
            
            if file_success:
                self.logger.info("Pipeline completado exitosamente")
                print("\nTransformación completada exitosamente\n")
                return True
            else:
                self.logger.error("Hubo problemas al completar el pipeline")
                print("\nHubo problemas al completar la transformación\n")
                return False
            
        except Exception as e:
            self.logger.exception(f"Error en el pipeline: {str(e)}")
            print(f"\nError: {str(e)}\n")
            return False
        
def main():
    """Función principal para ejecutar el transformador"""
    parser = argparse.ArgumentParser(description='Ejecutar NBA Playoffs Advanced Transformer')
    parser.add_argument('--input', type=str, help='Archivo CSV de entrada')
    parser.add_argument('--output', type=str, default='processed_data', help='Directorio de salida')
    
    # Argumentos para conexión a PostgreSQL
    parser.add_argument('--host', type=str, default='localhost', help='Host de PostgreSQL')
    parser.add_argument('--port', type=str, default='5432', help='Puerto de PostgreSQL')
    parser.add_argument('--db', type=str, default='nba_playoffs', help='Nombre de la base de datos')
    parser.add_argument('--user', type=str, default='postgres', help='Usuario de PostgreSQL')
    parser.add_argument('--password', type=str, default='123', help='Contraseña de PostgreSQL')
    parser.add_argument('--no-db', action='store_true', help='No guardar en base de datos')
    
    args = parser.parse_args()
    
    print("=== Transformador Avanzado NBA Playoffs ===")
    print(f"Directorio de salida: {args.output}")

     # Configuración de la base de datos
    db_config = None if args.no_db else {
        'host': args.host,
        'port': args.port,
        'database': args.db,
        'user': args.user,
        'password': args.password
    }
    
    if args.no_db:
        print("Modo sin base de datos activado. No se guardará en PostgreSQL.")
    else:
        if not DATABASE_AVAILABLE:
            print("Advertencia: Las librerías para PostgreSQL no están disponibles.")
            print("Se guardarán los resultados solo en archivos CSV.")
        else:
            print(f"Configuración de PostgreSQL: {args.host}:{args.port}/{args.db}")
    
    try:
        # Crear y ejecutar el transformador
        transformer = NBAPlayoffsAdvancedTransformer(
            input_file=args.input,
            output_dir=args.output,
            db_config=db_config
        )
        
        print("Ejecutando pipeline de transformación...")
        success = transformer.run_pipeline()
        
        if success:
            output_dir = Path(args.output)
            print("\n=== Transformación completada con éxito ===")
            print(f"Archivos generados en {output_dir}:")
            
            # Mostrar los archivos generados
            files = list(output_dir.glob('*.csv'))
            for file in files:
                file_size = file.stat().st_size // 1024  # Tamaño en KB
                print(f"  - {file.name} ({file_size} KB)")
                
            print("\nPuedes revisar los logs para más detalles.")
            return 0
        else:
            print("\n=== La transformación encontró problemas ===")
            print("Revisa los logs para más detalles.")
            return 1
            
    except Exception as e:
        print(f"\nError fatal: {str(e)}")
        print("\nDetalles del error:")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    print("Iniciando transformación avanzada de datos NBA...")
    sys.exit(main())
    
                

        
                
            
                

                
        

