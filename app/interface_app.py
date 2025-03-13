import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from tkinter import PhotoImage
import subprocess
import threading
import os
import datetime
from pathlib import Path
from PIL import Image, ImageTk



class NBAETLInterface:
    def __init__(self, root):
        self.root = root
        self.root.title("NBA Playoffs ETL Dashboard")
        self.root.geometry("800x600")
        
        # Variables para controlar el estado
        self.process_running = False
        
        # Crear la interfaz
        self.create_interface()
        
        # Inicializar terminal
        self.append_to_terminal("Sistema NBA Playoffs ETL Dashboard iniciado.")
        self.append_to_terminal("Listo para ejecutar comandos.")
        
        # Verificar directorios necesarios
        self.check_directories()

    def create_interface(self):
        # Colores
        primary_color = "#627baa"
        secondary_color = "#c81033"
        bg_color = "#627baa" #Color de fondo de la interfaz
        button_fg_color = "#ffffff"  # Color de texto de los botones
        console_fg_color = "#33ff33"  # Color de texto de la consola 
        button_color =  "#627baa"
        fg_color =  "#000000" 

        self.root.configure(bg=bg_color)
        
        # Frame principal
        main_frame = ttk.Frame(self.root, padding=2, style="Main.TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Cargar imagen
        original_image = Image.open("image/nba.png")
        resized_image = original_image.resize((70, 100), Image.LANCZOS)
        self.logo = ImageTk.PhotoImage(resized_image) 


        #Mostrar la imagen en un label

        logo_label = ttk.Label(main_frame, image=self.logo, background=bg_color)
        logo_label.pack(pady=10)

        
        # Panel de Control ETL
        control_frame = ttk.LabelFrame(main_frame, text="Panel de Control ETL", padding=5, style="Control.TLabelframe")
        control_frame.pack(fill=tk.X, pady=(0, 10), ipady=2, expand=True)
        
        # Botones
        self.extraction_btn = ttk.Button(
            control_frame,
            text="1. Ejecutar Extracción (test_extraction.py)",
            command=lambda: self.run_script("processed_data/scripts/test_extraction.py"),
            style="TButton",
            width=45
        )
        self.extraction_btn.pack(pady=2, padx=10, anchor="center")
        
        self.etl_btn = ttk.Button(
            control_frame,
            text="2. Ejecutar Transformación/Carga (nba_etl.py)",
            command=lambda: self.run_script("processed_data/scripts/nba_etl.py"),
            style="TButton",
            width=45
        )
        self.etl_btn.pack(pady=5, padx=10, anchor="center")
        
        self.advanced_btn = ttk.Button(
            control_frame,
            text="3. Ejecutar Transformación Avanzada (simplified_transformer.py)",
            command=lambda: self.run_script("processed_data/scripts/simplified_transformer.py"),
            style="TButton", 
            width=45
        )
        self.advanced_btn.pack(pady=5, padx=10, anchor="center")
        
        self.auto_btn = ttk.Button(
            control_frame,
            text="4. Configurar Automatización (automaticetl.py)",
            command=self.run_auto,
            style="TButton",
            width=45
        )
        self.auto_btn.pack(pady=5, padx=10, anchor="center")
        
        # Operaciones de Base de Datos
        db_frame = ttk.LabelFrame(main_frame, 
        text="Operaciones de Base de Datos", padding=10, style="Control.TLabelframe")
        db_frame.pack(fill=tk.X, pady=(0, 10), ipady=5, expand=True)
        
        db_buttons_frame = ttk.Frame(db_frame)
        db_buttons_frame.pack(pady=5)
        
        self.clear_db_btn = ttk.Button(
            db_buttons_frame,
            text="Limpiar Base de Datos",
            command=self.clear_database,
            style="TButton", 
            width=45
        )
        self.clear_db_btn.pack(side=tk.LEFT, padx=5, anchor="w")
        
        self.check_db_btn = ttk.Button(
            db_buttons_frame,
            text="Verificar Estado de BD",
            command=self.check_database,
            style="TButton",
            width=45
        )
        self.check_db_btn.pack(side=tk.LEFT, padx=5, anchor="w")
        
        # Terminal
        terminal_frame = ttk.LabelFrame(main_frame, text="Consola de Salida", padding=10, style="Control.TLabelframe")
        terminal_frame.pack(fill=tk.BOTH, expand=True)
        
        self.terminal = scrolledtext.ScrolledText(
            terminal_frame,
            bg='black',
            fg=console_fg_color,
            font=('Courier New', 10),
            wrap=tk.WORD,
            height=15  # Ajustar la altura de la consola


        )
        self.terminal.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        terminal_buttons_frame = ttk.Frame(terminal_frame)
        terminal_buttons_frame.pack(fill=tk.X)
        
        clear_terminal_btn = ttk.Button(
            terminal_buttons_frame,
            text="Limpiar Consola",
            command=self.clear_terminal,
            style="TButton"
        )
        clear_terminal_btn.pack(side=tk.LEFT, padx=(0, 5), fill=tk.X, expand=True)
        
        refresh_logs_btn = ttk.Button(
            terminal_buttons_frame,
            text="Mostrar Logs",
            command=self.show_logs,
            style="TButton"
        )
        refresh_logs_btn.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Pie de página
        footer_text = "NBA Playoffs ETL Dashboard | Desarrollado por Juan Diego Díaz Guzmán | " + datetime.datetime.now().strftime("%Y")
        ttk.Label(main_frame, text=footer_text, foreground=fg_color, background=bg_color).pack(pady=(10, 0))
        
        # Estilos
        style = ttk.Style()
        style.configure("Main.TFrame", background=bg_color)
        style.configure("Control.TLabelframe", background=bg_color, foreground=fg_color)
        style.configure("TButton", background=button_color, foreground=fg_color)
        style.map("TButton", background=[('active', secondary_color)])

    def check_directories(self):
        """Verifica que existan los directorios necesarios para el sistema"""
        dirs = ['logs', 'staging', 'processed_data']
        for dir_name in dirs:
            dir_path = Path(dir_name)
            if not dir_path.exists():
                dir_path.mkdir(parents=True)
                self.append_to_terminal(f"Creado directorio: {dir_name}")
    
    def append_to_terminal(self, text):
        """Añade texto a la terminal con timestamp"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.terminal.configure(state='normal')
        self.terminal.insert(tk.END, f"\n[{timestamp}] {text}")
        self.terminal.see(tk.END)
        self.terminal.configure(state='disabled')
    
    def clear_terminal(self):
        """Limpia la terminal"""
        self.terminal.configure(state='normal')
        self.terminal.delete(1.0, tk.END)
        self.terminal.configure(state='disabled')
        self.append_to_terminal("Terminal limpiada.")
        self.append_to_terminal("NBA Playoffs ETL Dashboard listo.")
    
    def run_script(self, script_name):
        """Ejecuta un script Python"""
        if self.process_running:
            messagebox.showinfo("En proceso", "Ya hay un proceso en ejecución.")
            return
        
        self.process_running = True
        self.disable_buttons()
        
        self.append_to_terminal(f"Ejecutando {script_name}...")
        
        # Iniciar la ejecución en un hilo separado
        threading.Thread(
            target=self._run_script_thread, 
            args=(script_name,), 
            daemon=True
        ).start()
    
    def _run_script_thread(self, script_name):
        """Hilo para ejecutar un script Python"""
        try:
            # Ejecutar el proceso
            process = subprocess.Popen(
                ['python', script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'  # Especificar la codificación utf-8
            )
            
            # Capturar salida
            stdout, stderr = process.communicate()
            
            # Actualizar la interfaz desde el hilo principal
            self.root.after(0, lambda: self._script_completed(script_name, process.returncode, stdout, stderr))
            
        except Exception as e:
            # Actualizar la interfaz desde el hilo principal en caso de error
            self.root.after(0, lambda: self._script_completed(script_name, 1, "", str(e)))
    
    def _script_completed(self, script_name, return_code, stdout, stderr):
        """Maneja la finalización de un script"""
        self.process_running = False
        self.enable_buttons()
        
        if return_code == 0:
            self.append_to_terminal(f"Proceso {script_name} completado con éxito.")
        else:
            self.append_to_terminal(f"Error en el proceso {script_name}. Código: {return_code}")
            if stderr:
                self.append_to_terminal(f"Error: {stderr}")
        
        # Mostrar la salida detallada
        if stdout:
            output_lines = stdout.split('\n')
            for line in output_lines:
                if line.strip():
                    self.append_to_terminal(f"  {line}")
    
    def run_auto(self):
        if self.process_running:
            messagebox.showinfo("En proceso", "Ya hay un proceso en ejecución.")
            return
        
        # Preguntar por el modo de ejecución
        result = messagebox.askyesno(
            "Configuración de Automatización",
            "¿Desea ejecutar en modo programado?\n\n"
            "Sí: Ejecutar como servicio programado diariamente\n"
            "No: Ejecutar una sola vez sin programar"
        )
        
        self.process_running = True
        self.disable_buttons()
        
        self.append_to_terminal("Ejecutando automaticetl.py...")
        
        # Preparar el comando
        cmd = ['python', 'processed_data/scripts/automaticetl.py']
        if not result:
            cmd.append('--no-schedule')
            self.append_to_terminal("Ejecutando en modo sin programación...")
        else:
            self.append_to_terminal("Ejecutando en modo programado...")
        
        # Iniciar la ejecución en un hilo separado
        threading.Thread(target=self._run_auto_thread, args=(cmd,), daemon=True).start()
    
    def _run_auto_thread(self, cmd):
        try:
            # Ejecutar el proceso
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'  
            )
            
            # Capturar salida
            stdout, stderr = process.communicate()
            
            # Actualizar la interfaz desde el hilo principal
            self.root.after(0, lambda: self._script_completed("automaticetl.py", process.returncode, stdout, stderr))
            
        except Exception as e:
            # Actualizar la interfaz desde el hilo principal en caso de error
            self.root.after(0, lambda: self._script_completed("automaticetl.py", 1, "", str(e)))
    
    def clear_database(self):
        #Limpieza de base de datos
        self.append_to_terminal("Preparando script para limpiar la base de datos...")
        
        # Preguntar confirmación
        result = messagebox.askyesno(
            "Confirmar limpieza",
            "¿Está seguro de que desea limpiar todas las tablas de la base de datos?\n\n"
            "Esta acción no se puede deshacer.",
            icon='warning'
        )
        
        if not result:
            self.append_to_terminal("Operación de limpieza cancelada.")
            return
        
        # Crear script temporal
        script_content = """
import psycopg2

# Configuración de la base de datos
db_config = {
    "host": "localhost",
    "port": "5432",
    "database": "nba_playoffs",
    "user": "postgres",
    "password": "123"
}

try:
    # Conectar a la base de datos
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    
    # Crear cursor
    cursor = conn.cursor()
    
    # Truncar tablas
    tables = [
        "nba_playoffs_detailed",
        "nba_playoffs_season_summary",
        "nba_playoffs_team_summary",
        "nba_playoffs_advanced"
    ]
    
    for table in tables:
        print(f"Truncando tabla {table}...")
        cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
    
    # Commit cambios
    conn.commit()
    
    # Verificar resultados
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} registros")
    
    # Cerrar conexión
    cursor.close()
    conn.close()
    
    print("Tablas limpiadas correctamente.")
    
except Exception as e:
    print(f"Error: {str(e)}")
"""
        
        # Guardar script temporal
        script_path = "temp_clear_db.py"
        with open(script_path, "w", encoding='utf-8') as f:
            f.write(script_content)
        
        # Ejecutar script
        self.run_script(script_path)
    
    def check_database(self):
        """Verifica el estado de la base de datos"""
        self.append_to_terminal("Preparando script para verificar la base de datos...")
        
        # Crear script temporal
        script_content = """
import psycopg2

# Configuración de la base de datos
db_config = {
    "host": "localhost",
    "port": "5432",
    "database": "nba_playoffs",
    "user": "postgres",
    "password": "123"
}

try:
    # Conectar a la base de datos
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    
    # Crear cursor
    cursor = conn.cursor()
    
    # Verificar tablas
    tables = [
        "nba_playoffs_detailed",
        "nba_playoffs_season_summary",
        "nba_playoffs_team_summary",
        "nba_playoffs_advanced"
    ]
    
    print("Estado de la base de datos:")
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} registros")
        except Exception as e:
            print(f"{table}: Error al acceder - {str(e)}")
    
    # Muestreo de datos si hay registros
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"\\nMuestra de {table} (primeros 2 registros):")
                cursor.execute(f"SELECT * FROM {table} LIMIT 2;")
                columns = [desc[0] for desc in cursor.description]
                print(f"Columnas: {', '.join(columns)}")
                
                rows = cursor.fetchall()
                for i, row in enumerate(rows):
                    print(f"Registro {i+1}:", end=" ")
                    # Mostrar solo primeros 3 valores para no saturar la salida
                    print(f"{row[:3]}...")
        except Exception as e:
            pass
    
    # Cerrar conexión
    cursor.close()
    conn.close()
    
    print("\\nVerificación completada.")
    
except Exception as e:
    print(f"Error al conectar a la base de datos: {str(e)}")
"""
        
        # Guardar script temporal
        script_path = "temp_check_db.py"
        with open(script_path, "w", encoding='utf-8') as f:
            f.write(script_content)
        
        # Ejecutar script
        self.run_script(script_path)
    
    def show_logs(self):
        self.append_to_terminal("Buscando archivos de log recientes...")
        
        logs_dir = Path("logs")
        if not logs_dir.exists():
            self.append_to_terminal("No se encontró el directorio de logs.")
            return
        
        # Buscar archivos de log
        log_files = []
        for pattern in ["repo_extraction_*.log", "etl_process_*.log", "etl_automation_*.log", "advanced_transform_*.log"]:
            log_files.extend(logs_dir.glob(pattern))
        
        if not log_files:
            self.append_to_terminal("No se encontraron archivos de log.")
            return
        
        # Ordenar por fecha de modificación (el más reciente primero)
        log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Mostrar el log más reciente
        latest_log = log_files[0]
        self.append_to_terminal(f"Mostrando contenido de: {latest_log.name}")
        
        try:
            with open(latest_log, "r", encoding='latin-1') as f:
                # Leer las últimas 10 líneas
                lines = f.readlines()
                last_lines = lines[-20:] if len(lines) > 20 else lines
                
                for line in last_lines:
                    self.append_to_terminal(f"  {line.strip()}")
                
                self.append_to_terminal(f"Mostradas las últimas {len(last_lines)} líneas de {latest_log.name}")
        except Exception as e:
            self.append_to_terminal(f"Error al leer el archivo de log: {str(e)}")
    
    def disable_buttons(self):
        self.extraction_btn.configure(state='disabled')
        self.etl_btn.configure(state='disabled')
        self.advanced_btn.configure(state='disabled')
        self.auto_btn.configure(state='disabled')
        self.clear_db_btn.configure(state='disabled')
        self.check_db_btn.configure(state='disabled')
    
    def enable_buttons(self):
        self.extraction_btn.configure(state='normal')
        self.etl_btn.configure(state='normal')
        self.advanced_btn.configure(state='normal')
        self.auto_btn.configure(state='normal')
        self.clear_db_btn.configure(state='normal')
        self.check_db_btn.configure(state='normal')
    
    def update_status_indicator(self, canvas, status):
        # Limpiar canvas
        canvas.delete("all")
        
        # Colores de estado
        colors = {
            "idle": "#999",
            "running": "#ffcc00",
            "success": "#33cc33",
            "error": "#ff3333"
        }
        
        # Dibujar círculo
        canvas.create_oval(2, 2, 10, 10, fill=colors[status], outline="")

if __name__ == "__main__":
    root = tk.Tk()
    app = NBAETLInterface(root)
    root.mainloop()