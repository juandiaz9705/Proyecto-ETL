#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Importar la clase de extracción
from repo_extraction import RepositoryToStaging

def test_extraction():
    print("=== PRUEBA DE EXTRACCIÓN DE DATOS NBA ===")
    
    # Configuración
    staging_dir = Path('staging')
    
    # Crear instancia del extractor
    extractor = RepositoryToStaging(
        source_repo_url="https://github.com/NocturneBear/NBA-Data-2010-2024",
        staging_dir=staging_dir,
        log_dir='logs'
    )
    
    # Ejecutar extracción
    success = extractor.run()
    
    if success:
        print("\n✅ Extracción completada con éxito!")
        
        # Verificar archivos generados
        print("\nArchivos en staging:")
        for dirpath, dirnames, filenames in os.walk(staging_dir):
            for filename in filenames:
                print(f"  - {os.path.join(dirpath, filename)}")
        
        # Verificar si el archivo principal está listo para ETL
        main_file = Path('play_off_totals_2010_2024.csv')
        if main_file.exists():
            print(f"\n✅ Archivo principal listo para ETL: {main_file}")
            print(f"  - Tamaño: {main_file.stat().st_size} bytes")
        else:
            print(f"\n❌ Archivo principal no encontrado: {main_file}")
    else:
        print("\n❌ La extracción falló. Revisa los logs para más detalles.")
    
    return success

if __name__ == "__main__":
    sys.exit(0 if test_extraction() else 1)