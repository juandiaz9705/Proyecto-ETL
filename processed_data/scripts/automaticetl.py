"""
Autor: Juan Diego Díaz Guzmán
Fecha: 13-03-2025
"""

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
            'staging_dir': 'data/staging',
            'extract_script': 'processed_data/scripts/test_extraction.py',  # Asumiendo que renombraste test_extraction.py
            'max_retries': 3,
            'retry_delay': 300  # 5 minutos entre reintentos
        }
        
       
        if config:
            self.config.update(config)
            
        # Inicializar logger
        self.logger = self.setup_logging()
        
        # Crear directorios necesarios
        os.makedirs('logs', exist_ok=True)
        os.makedirs(self.config['staging_dir'], exist_ok=True)
        os.makedirs('data/processed_data', exist_ok=True)

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
                
            # Paso 2: Ejecutar ETL en los datos extraídos
            etl = NBAPlayoffsETL(
                db_config=self.config['db_config'],
                staging_dir=self.config['staging_dir']
            )
            
            etl_success = etl.run_pipeline()
            
            if not etl_success:
                self.logger.error("Fallo en el procesamiento ETL.")
                return False
                
            # Paso 3: Crear archivo de control
            self.logger.info("ETL ejecutado exitosamente")
            control_file = Path('logs') / f'etl_automation_success_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            with open(control_file, 'w') as f:
                f.write(f"ETL automatizado completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Repositorio fuente: {self.config['repo_url']}\n")
            
            return True

        except Exception as e:
            self.logger.error(f"Error inesperado en la automatización: {str(e)}")
            return False

    def run_with_retry(self):
  
        self.logger.info(f"Iniciando ETL con {self.config['max_retries']} intentos máximos")
        
        for attempt in range(1, self.config['max_retries'] + 1):
            self.logger.info(f"Intento {attempt} de {self.config['max_retries']}")
            
            success = self.run_etl()
            if success:
                self.logger.info(f"ETL completado exitosamente en el intento {attempt}")
                return True
                
            if attempt < self.config['max_retries']:
                self.logger.warning(f"Reintentando en {self.config['retry_delay']} segundos...")
                time.sleep(self.config['retry_delay'])
            
        self.logger.error(f"ETL falló después de {self.config['max_retries']} intentos")
        return False

    def start(self):
      
        self.logger.info("Iniciando sistema de automatización ETL")
        
        # Programar ejecución diaria a la hora configurada
        schedule.every().day.at(self.config['schedule_time']).do(self.run_with_retry)
        self.logger.info(f"ETL programado para ejecutarse diariamente a las {self.config['schedule_time']}")
        
        
        # Ejecutar inmediatamente la primera vez (opcional)
        self.logger.info("Ejecutando ETL inicial...")
        self.run_with_retry()
        
        # Loop principal
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Verificar cada minuto
            except KeyboardInterrupt:
                self.logger.info("Deteniendo sistema de automatización ETL")
                break
            except Exception as e:
                self.logger.error(f"Error en loop principal: {str(e)}")
                time.sleep(300)  # Esperar 5 minutos en caso de error


def main():
    parser = argparse.ArgumentParser(description='Automatización de ETL para NBA Playoffs')
    parser.add_argument('--time', type=str, default='02:00', help='Hora de ejecución diaria (HH:MM)')
    parser.add_argument('--repo', type=str, default='https://github.com/NocturneBear/NBA-Data-2010-2024', 
                        help='URL del repositorio fuente')
    parser.add_argument('--no-schedule', action='store_true', help='Ejecutar una vez sin programar')
    
    args = parser.parse_args()
    
    # Crear configuración personalizada basada en argumentos
    config = {
        'schedule_time': args.time,
        'repo_url': args.repo
    }
    
    # Iniciar automatización
    automation = ETLAutomation(config)
    
    if args.no_schedule:
        print("Ejecutando ETL una vez sin programación...")
        success = automation.run_with_retry()
        return 0 if success else 1
    else:
        try:
            automation.start()
            return 0
        except KeyboardInterrupt:
            print("\nDetención manual del sistema.")
            return 0


if __name__ == "__main__":
    exit(main())