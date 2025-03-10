import os
import shutil
import logging
import argparse
import subprocess
import pandas as pd
from datetime import datetime
from pathlib import Path

#Clase de extracción de repositorio
class RepositoryToStaging:

    def __init__(self, source_repo_url, staging_dir, log_dir='logs'):
      #Inicialización del repositorio
        
        self.source_repo_url = source_repo_url
        self.staging_dir = Path(staging_dir)
        self.log_dir = Path(log_dir)
        self.temp_dir = Path('temp_repo')
        
        # Crear directorios necesarios
        self.staging_dir.mkdir(exist_ok=True, parents=True)
        self.log_dir.mkdir(exist_ok=True, parents=True)
        
        # Configuración de logging
        self.setup_logging()
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        log_file = self.log_dir / f'repo_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Proceso de extracción iniciado")
    
    def clone_repository(self):
        try:
            # Eliminar directorio temporal si existe
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
            
            self.logger.info(f"Clonando repositorio desde {self.source_repo_url}")
            result = subprocess.run(
                ['git', 'clone', self.source_repo_url, str(self.temp_dir)],
                check=True,
                capture_output=True,
                text=True
            )
            
            self.logger.info(f"Repositorio clonado exitosamente en {self.temp_dir}")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error al clonar repositorio: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado al clonar repositorio: {str(e)}")
            return False
    
    def find_data_files(self, data_patterns=None):

        if data_patterns is None:
            data_patterns = ['*.csv']
            
        found_files = []
        try:
            for pattern in data_patterns:
                for file_path in self.temp_dir.glob('**/' + pattern):
                    if file_path.is_file():
                        found_files.append(file_path)
            
            self.logger.info(f"Se encontraron {len(found_files)} archivos de datos")
            for file in found_files:
                self.logger.info(f"  - {file}")
                
            return found_files
            
        except Exception as e:
            self.logger.error(f"Error al buscar archivos de datos: {str(e)}")
            return []
    
    def copy_to_staging(self, file_paths):

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            staging_subdir = self.staging_dir / f'extract_{timestamp}'
            staging_subdir.mkdir(exist_ok=True)
            
            self.logger.info(f"Copiando archivos al directorio de staging: {staging_subdir}")
            
            copied_files = []
            for src_path in file_paths:
                dest_path = staging_subdir / src_path.name
                shutil.copy2(src_path, dest_path)
                copied_files.append(dest_path)
                self.logger.info(f"Copiado: {src_path} -> {dest_path}")
            
            # Crear archivo de control
            control_file = staging_subdir / '_CONTROL.txt'
            with open(control_file, 'w') as f:
                f.write(f"Extracción completada: {datetime.now().isoformat()}\n")
                f.write(f"Repositorio fuente: {self.source_repo_url}\n")
                f.write(f"Archivos extraídos: {len(copied_files)}\n")
                for file in copied_files:
                    file_size = os.path.getsize(file)
                    f.write(f"  - {file.name} ({file_size} bytes)\n")
            
            self.logger.info(f"Creado archivo de control: {control_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al copiar archivos al staging: {str(e)}")
            return False
    
    def validate_data_files(self, file_paths):

        validation_stats = {}
        
        try:
            self.logger.info("Validando archivos de datos...")
            
            for file_path in file_paths:
                if not file_path.exists():
                    self.logger.warning(f"El archivo {file_path} no existe")
                    continue
                    
                if file_path.suffix.lower() == '.csv':
                    try:
                        # Leer solo las primeras filas para verificar estructura
                        df_sample = pd.read_csv(file_path, nrows=5)
                        row_count = len(pd.read_csv(file_path))
                        
                        # Guardar estadísticas
                        validation_stats[file_path.name] = {
                            'rows': row_count,
                            'columns': len(df_sample.columns),
                            'column_names': list(df_sample.columns),
                            'valid': True
                        }
                        
                        self.logger.info(f"Archivo {file_path.name} validado: {row_count} filas, {len(df_sample.columns)} columnas")
                    except Exception as e:
                        validation_stats[file_path.name] = {
                            'valid': False,
                            'error': str(e)
                        }
                        self.logger.error(f"Error al validar {file_path.name}: {str(e)}")
            
            return validation_stats
            
        except Exception as e:
            self.logger.error(f"Error en la validación de archivos: {str(e)}")
            return {'error': str(e)}
    
    def prepare_for_etl(self, staging_dir):
        try:
            # Crear directorio processed_data si no existe
            processed_dir = Path('processed_data')
            processed_dir.mkdir(exist_ok=True)
            
            # Buscar el archivo principal de playoffs
            playoff_file = None
            for file_path in staging_dir.glob('*.csv'):
                if 'play_off' in file_path.name.lower():
                    playoff_file = file_path
                    break
            
            if not playoff_file:
                self.logger.warning("No se encontró el archivo de playoffs en el staging")
                return False
            
            # Copiar a la ubicación esperada por el ETL
            target_path = Path('play_off_totals_2010_2024.csv')
            shutil.copy2(playoff_file, target_path)
            self.logger.info(f"Archivo preparado para ETL: {playoff_file} -> {target_path}")
            
            # Crear un archivo detallado procesado para la aplicación Flask
            try:
                df = pd.read_csv(target_path)
                processed_file = processed_dir / 'playoffs_detailed_processed.csv'
                df.to_csv(processed_file, index=False)
                self.logger.info(f"Archivo procesado creado: {processed_file}")
            except Exception as e:
                self.logger.error(f"Error al crear archivo procesado: {str(e)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al preparar datos para ETL: {str(e)}")
            return False
    
    def cleanup(self):
        """Limpia recursos temporales"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Directorio temporal {self.temp_dir} eliminado")
        except Exception as e:
            self.logger.error(f"Error al limpiar recursos temporales: {str(e)}")
    
    def download_specific_file(self, file_url, output_path):

        try:
            # Convertir URL de GitHub web a URL raw
            # Ejemplo: https://github.com/user/repo/blob/main/file.csv -> https://raw.githubusercontent.com/user/repo/main/file.csv
            raw_url = file_url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
            
            self.logger.info(f"Descargando archivo desde: {raw_url}")
            
            import requests
            response = requests.get(raw_url)
            response.raise_for_status()  # Lanzar excepción si hay error HTTP
            
            # Guardar el archivo
            with open(output_path, 'wb') as f:
                f.write(response.content)
                
            self.logger.info(f"Archivo descargado correctamente a: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al descargar archivo: {str(e)}")
            return False

    def run(self):
  
        try:
            self.logger.info("Iniciando proceso de extracción de repositorio a staging")
            
            # Crear directorio temporal si no existe
            if not self.temp_dir.exists():
                self.temp_dir.mkdir(exist_ok=True)
            
            # URL específica del archivo de datos NBA
            nba_file_url = "https://github.com/NocturneBear/NBA-Data-2010-2024/blob/main/play_off_totals_2010_2024.csv"
            nba_file_path = self.temp_dir / "play_off_totals_2010_2024.csv"
            
            # Descargar archivo específico (alternativa a clonar todo el repositorio)
            if not self.download_specific_file(nba_file_url, nba_file_path):
                self.logger.error("Fallo al descargar archivo específico. Intentando clonar repositorio completo.")
                
                # Paso 1 (alternativo): Clonar repositorio
                if not self.clone_repository():
                    self.logger.error("Fallo al clonar repositorio. Abortando proceso.")
                    return False
            
            # Paso 2: Establecer archivos de datos (específicamente o encontrados)
            data_files = [nba_file_path] if nba_file_path.exists() else self.find_data_files(data_patterns=['*.csv'])
            
            # Paso 3: Copiar al staging
            if not self.copy_to_staging(data_files):
                self.logger.error("Fallo al copiar archivos al staging. Abortando proceso.")
                self.cleanup()
                return False
            
            # Paso 4: Validar archivos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            current_staging = self.staging_dir / f'extract_{timestamp}'
            copied_files = list(current_staging.glob('*.*'))
            validation_results = self.validate_data_files(copied_files)
            
            # Guardar resultados de validación
            validation_file = current_staging / '_VALIDATION.json'
            pd.Series(validation_results).to_json(validation_file)
            self.logger.info(f"Resultados de validación guardados en {validation_file}")
            
            # Paso 5: Preparar para ETL
            if not self.prepare_for_etl(current_staging):
                self.logger.warning("Advertencia: Posibles problemas al preparar datos para ETL")
            
            # Paso 6: Limpieza
            self.cleanup()
            
            self.logger.info("Proceso de extracción de repositorio a staging completado con éxito")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en el proceso de extracción: {str(e)}")
            self.cleanup()
            return False


def main():
    parser = argparse.ArgumentParser(description='Extracción de datos desde repositorio a staging')
    parser.add_argument('--repo', type=str, required=True, help='URL del repositorio fuente')
    parser.add_argument('--staging', type=str, default='staging', help='Directorio de staging')
    parser.add_argument('--log-dir', type=str, default='logs', help='Directorio de logs')
    
    args = parser.parse_args()
    
    extractor = RepositoryToStaging(
        source_repo_url=args.repo,
        staging_dir=args.staging,
        log_dir=args.log_dir
    )
    
    success = extractor.run()
    
    if success:
        print("\n✅ Proceso completado con éxito\n")
        return 0
    else:
        print("\n❌ El proceso falló. Revise los logs para más detalles.\n")
        return 1


if __name__ == "__main__":
    exit(main())