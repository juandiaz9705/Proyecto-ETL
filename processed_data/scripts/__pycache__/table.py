
import psycopg2
import pandas as pd
from pathlib import Path
import sys

# Configuración de la base de datos
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'nba_playoffs',
    'user': 'postgres',
    'password': '123'
}

def check_connection():
    """Verifica la conexión a PostgreSQL"""
    print("Verificando conexión a PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexión exitosa a PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        print(f"Error de conexión: {e}")
        return False

def get_table_info(table_name):
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Verificar si la tabla existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        
        if not cur.fetchone()[0]:
            print(f"❌ La tabla '{table_name}' no existe")
            conn.close()
            return False
        
        # Obtener columnas de la tabla
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = cur.fetchall()
        print(f"\nEstructura de la tabla '{table_name}':")
        print("=" * 60)
        print(f"{'Columna':<20} {'Tipo':<15} {'Nullable':<10}")
        print("-" * 60)
        
        for col in columns:
            print(f"{col[0]:<20} {col[1]:<15} {col[2]:<10}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error al obtener información de la tabla: {e}")
        return False

def fix_team_summary_table():
    """Corrige la tabla nba_playoffs_team_summary"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Primero mostrar la estructura actual
        print("\nEstructura original de la tabla:")
        get_table_info('nba_playoffs_team_summary')
        
        # Preguntar confirmación para recrear la tabla
        confirm = input("\n¿Quieres recrear la tabla nba_playoffs_team_summary? (s/n): ")
        if confirm.lower() != 's':
            print("Operación cancelada")
            conn.close()
            return False
        
        # Recrear la tabla
        cur.execute("""
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
        """)
        
        conn.commit()
        print("\n✅ Tabla nba_playoffs_team_summary recreada exitosamente")
        
        # Mostrar nueva estructura
        print("\nNueva estructura de la tabla:")
        get_table_info('nba_playoffs_team_summary')
        
        # Intentar cargar datos desde CSV
        csv_path = Path('processed_data/team_summary.csv')
        if csv_path.exists():
            print(f"\nEncontramos el archivo {csv_path}")
            confirm = input("¿Quieres importar datos desde este CSV? (s/n): ")
            if confirm.lower() == 's':
                df = pd.read_csv(csv_path)
                print(f"Cargando {len(df)} registros desde el CSV...")
                
                # Renombrar columnas si es necesario
                if 'Unnamed: 0' in df.columns:
                    df.rename(columns={'Unnamed: 0': 'team_name'}, inplace=True)
                
                # Mapping de columnas para asegurar compatibilidad
                column_mapping = {
                    'TEAM_NAME': 'team_name',
                    'PTS': 'pts',
                    'WIN_RATE': 'win_rate',
                    'AST': 'ast',
                    'OFFENSIVE_EFFICIENCY': 'offensive_efficiency',
                    'DEFENSIVE_RATING': 'defensive_rating',
                    'PLAYOFF_EFFICIENCY': 'playoff_efficiency'
                }
                
                # Renombrar columnas para que coincidan con la tabla
                for old_name, new_name in column_mapping.items():
                    if old_name in df.columns:
                        df.rename(columns={old_name: new_name}, inplace=True)
                
                # Guardar en PostgreSQL
                from sqlalchemy import create_engine
                engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
                
                # Seleccionar solo las columnas que existen en la tabla
                df = df[['team_name', 'pts', 'win_rate', 'ast', 'offensive_efficiency', 'defensive_rating', 'playoff_efficiency']]
                
                # Guardar a PostgreSQL
                df.to_sql('nba_playoffs_team_summary', engine, if_exists='append', index=False)
                print("✅ Datos importados exitosamente")
                
                # Verificar registros
                cur.execute("SELECT COUNT(*) FROM nba_playoffs_team_summary")
                count = cur.fetchone()[0]
                print(f"Ahora hay {count} registros en la tabla")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error al corregir la tabla: {e}")
        return False

def fix_season_summary_table():
    """Corrige la tabla nba_playoffs_season_summary"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Primero mostrar la estructura actual
        print("\nEstructura original de la tabla:")
        get_table_info('nba_playoffs_season_summary')
        
        # Preguntar confirmación para recrear la tabla
        confirm = input("\n¿Quieres recrear la tabla nba_playoffs_season_summary? (s/n): ")
        if confirm.lower() != 's':
            print("Operación cancelada")
            conn.close()
            return False
        
        # Recrear la tabla
        cur.execute("""
        DROP TABLE IF EXISTS nba_playoffs_season_summary;
        CREATE TABLE nba_playoffs_season_summary (
            season_year VARCHAR(10) PRIMARY KEY,
            pts FLOAT,
            fg3m FLOAT,
            ast FLOAT,
            offensive_efficiency FLOAT,
            defensive_rating FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        print("\n✅ Tabla nba_playoffs_season_summary recreada exitosamente")
        
        # Mostrar nueva estructura
        print("\nNueva estructura de la tabla:")
        get_table_info('nba_playoffs_season_summary')
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error al corregir la tabla: {e}")
        return False

def main():
    """Función principal"""
    print("=" * 60)
    print("DIAGNÓSTICO Y CORRECCIÓN DE TABLAS POSTGRESQL - NBA PLAYOFFS")
    print("=" * 60)
    
    # Verificar conexión
    if not check_connection():
        print("No se puede continuar sin conexión a PostgreSQL")
        return 1
    
    print("\nTablas principales para verificar:")
    print("1. nba_playoffs_team_summary")
    print("2. nba_playoffs_season_summary")
    print("3. nba_playoffs_advanced")
    print("4. Verificar todas")
    print("5. Corregir tabla nba_playoffs_team_summary")
    print("6. Corregir tabla nba_playoffs_season_summary")
    print("0. Salir")
    
    choice = input("\nElige una opción (0-6): ")
    
    if choice == "1":
        get_table_info('nba_playoffs_team_summary')
    elif choice == "2":
        get_table_info('nba_playoffs_season_summary')
    elif choice == "3":
        get_table_info('nba_playoffs_advanced')
    elif choice == "4":
        get_table_info('nba_playoffs_team_summary')
        get_table_info('nba_playoffs_season_summary')
        get_table_info('nba_playoffs_advanced')
    elif choice == "5":
        fix_team_summary_table()
    elif choice == "6":
        fix_season_summary_table()
    elif choice == "0":
        print("Saliendo...")
    else:
        print("Opción no válida")
    
    print("\n¡Gracias por usar esta herramienta!")
    return 0

if __name__ == "__main__":
    exit(main())