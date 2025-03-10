import schedule
import time
from datetime import datetime
import logging
import sys
import os
from pathlib import Path
import argparse
import subprocess
from nba_etl import NBAPlayoffsETL


class ETLAutomation:
   
    def __init__(self, config=None):
     
        # Configuración predeterminada
        self.config = {
            'db_config': {
                'host': 'localhost',
                'port': '5432',
                'database': 'nba_playoffs',
                'user': 'postgres',
                'password': '123'
            },
            'schedule_time': '02:00',  # Hora de ejecución diaria (formato HH:MM)
            'repo_url': 'https://github.com/NocturneBear/NBA-Data-2010-2024',
            'staging_dir': 'staging',
            'extract_script': 'repository_to_staging.py',  # Asumiendo que renombraste test_extraction.py
            'max_retries': 3,
            'retry_delay': 300  # 5 minutos entre reintentos
        }
        
        # Actualizar con configuración personalizada si se proporciona
        if config:
            self.config.update(config)
            
        # Inicializar logger
        self.logger = self.setup_logging()
        
        # Crear directorios necesarios
        os.makedirs('logs', exist_ok=True)
        os.makedirs(self.config['staging_dir'], exist_ok=True)
        os.makedirs('processed_data', exist_ok=True)

    def setup_logging(self):
     
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f'etl_automation_{datetime.now().strftime("%Y%m%d")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)

    def run_extraction(self):
     
        try:
            self.logger.info("Iniciando proceso de extracción de repositorio")
            
            # Comando para ejecutar el script de extracción
            cmd = [
                sys.executable,
                self.config['extract_script'],
                '--repo', self.config['repo_url'],
                '--staging', self.config['staging_dir']
            ]
            
            self.logger.info(f"Ejecutando comando: {' '.join(cmd)}")
            
            # Ejecutar proceso y capturar salida
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            
            self.logger.info("Proceso de extracción completado")
            self.logger.info(f"Salida: {result.stdout}")
            
            if result.returncode == 0:
                return True
            else:
                self.logger.error(f"Error en la extracción: {result.stderr}")
                return False
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error al ejecutar script de extracción: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado en la extracción: {str(e)}")
            return False

    def run_etl(self):
     
        self.logger.info("Iniciando ejecución programada del ETL")

        try:
            # Paso 1: Extraer datos del repositorio
            extraction_success = self.run_extraction()
            if not extraction_success:
                self.logger.error("Fallo en la extracción de datos. Abortando ETL.")
                return False
                
            self.logger.info("Extracción completada. Continuando con transformación y carga.")
                
            