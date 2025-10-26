import logging
import sys
import os
import mysql.connector
from config import DB_CONFIG
import tkinter as tk
from tkinter import messagebox
import re


# הגדרת לוגים לקובץ
def setup_logging():
    # יצירת שם קובץ לוג ליד ה-EXE
    if getattr(sys, 'frozen', False):
        # אם רץ כ-EXE
        log_dir = os.path.dirname(sys.executable)
    else:
        # אם רץ כ-Python script
        log_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_file = os.path.join(log_dir, 'mysql_runner.log')
    
    # הגדרת לוגר
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # גם לקונסול אם קיים
        ]
    )

# הפעלת הלוגר
setup_logging()
logger = logging.getLogger(__name__)


def parse_execution_order_file(order_file_path):
    """קורא את קובץ סדר ההרצה ומחזיר רשימה מסודרת של קבצים
    
    תומך בפורמט 2 בלבד - נתיבים פשוטים:
    1. folder1/script1.sql
    2. folder1/script2.sql
    3. folder2/script3.sql
    
    או בלי מספור:
    folder1/script1.sql
    folder1/script2.sql
    folder2/script3.sql
    """
    logger.info(f"Parsing execution order file: {order_file_path}")
    execution_order = []
    
    if not os.path.exists(order_file_path):
        logger.error(f"Order file not found: {order_file_path}")
        raise FileNotFoundError(f"Order file not found: {order_file_path}")
    
    with open(order_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        
        # דילוג על שורות ריקות והערות
        if not line or line.startswith('#') or line.startswith('//'):
            continue
        
        # בדיקה שהשורה מסתיימת ב-.sql
        if not line.endswith('.sql'):
            logger.warning(f"Line {line_num}: '{line}' does not end with .sql - skipping")
            continue
        
        # הסרת מספור בתחילת השורה אם קיים
        clean_line = line
        
        # זיהוי מספור: "1. ", "2. ", וכו'
        if '. ' in line:
            parts = line.split('. ', 1)
            if len(parts) == 2 and parts[0].strip().isdigit():
                clean_line = parts[1]
        
        # זיהוי מספור עם Tab: "1	"
        elif '\t' in line:
            parts = line.split('\t', 1)
            if len(parts) == 2 and parts[0].strip().isdigit():
                clean_line = parts[1]
        
        # זיהוי מספור עם רווח: "1 "
        elif ' ' in line:
            parts = line.split(' ', 1)
            if len(parts) == 2 and parts[0].strip().isdigit():
                clean_line = parts[1]
        
        clean_line = clean_line.strip()
        
        # חלוקה לתיקייה וקובץ לפי נתיב
        if '/' in clean_line or '\\' in clean_line:
            # נתיב מלא - המרה לפורמט אחיד
            path_parts = clean_line.replace('\\', '/').split('/')
            if len(path_parts) >= 2:
                folder_name = path_parts[-2]  # התיקייה האחרונה בנתיב
                filename = path_parts[-1]     # שם הקובץ
            else:
                # אם אין תיקייה בנתיב
                folder_name = 'scripts'
                filename = clean_line
        else:
            # רק שם קובץ בלי נתיב - נשתמש בתיקייה ברירת מחדל
            folder_name = 'scripts'
            filename = clean_line
        
        # וידוא שהקובץ מסתיים ב-.sql
        if filename.endswith('.sql'):
            execution_order.append((folder_name, filename))
            logger.debug(f"Line {line_num}: Added [{folder_name}] {filename}")
        else:
            logger.warning(f"Line {line_num}: '{filename}' is not a SQL file - skipping")
    
    logger.info(f"Parsed {len(execution_order)} scripts from order file")
    return execution_order


def scan_and_validate_scripts(root_folder, execution_order):
    """בודק שכל הקבצים מהרשימה קיימים בתיקיות"""
    logger.info(f"Scanning and validating scripts in: {root_folder}")
    found_scripts = []
    missing_scripts = []
    
    for folder_name, filename in execution_order:
        folder_path = os.path.join(root_folder, folder_name)
        script_path = os.path.join(folder_path, filename)
        
        if os.path.exists(script_path):
            found_scripts.append((folder_name, script_path))
            logger.debug(f"Found script: [{folder_name}] {filename}")
        else:
            missing_scripts.append((folder_name, filename))
            logger.warning(f"Missing script: [{folder_name}] {filename}")
    
    logger.info(f"Validation complete: {len(found_scripts)} found, {len(missing_scripts)} missing")
    return found_scripts, missing_scripts


def display_execution_plan(execution_order, found_scripts, missing_scripts):
    """מציג את תוכנית ההרצה"""
    logger.info(f"Displaying execution plan - {len(execution_order)} scripts total")
    print(f"\n=== Execution Plan - {len(execution_order)} scripts total ===")
    
    for i, (folder_name, filename) in enumerate(execution_order, 1):
        status = "✅" if (folder_name, filename) not in [(f, fn) for f, fn in missing_scripts] else "❌"
        message = f"{i:3d}. [{folder_name}] {filename} {status}"
        print(message)
        logger.info(message)
    
    if missing_scripts:
        logger.warning(f"Missing scripts ({len(missing_scripts)}):")
        print(f"\n⚠️ Missing scripts ({len(missing_scripts)}):")
        for folder_name, filename in missing_scripts:
            message = f"   ❌ [{folder_name}] {filename}"
            print(message)
            logger.warning(message)
    
    # הודעת messagebox
    summary_message = f"Execution Plan:\n{len(found_scripts)} scripts found\n{len(missing_scripts)} scripts missing\n\n"
    
    if missing_scripts:
        summary_message += "Missing scripts:\n"
        for folder_name, filename in missing_scripts[:5]:  # הצג רק 5 ראשונים
            summary_message += f"• [{folder_name}] {filename}\n"
        if len(missing_scripts) > 5:
            summary_message += f"... and {len(missing_scripts) - 5} more"
    
    messagebox.showinfo("Execution Plan", summary_message)


def execute_script_statements(cursor, connection, script_path, script_content):
    """מבצע את כל ההצהרות SQL בסקריפט אחד"""
    logger.debug(f"Executing statements from: {script_path}")
    
    # בדיקה אם הסקריפט מכיל טרנזקציה מפורשת
    content_upper = script_content.upper().strip()
    has_explicit_transaction = (
        'START TRANSACTION' in content_upper or 
        'BEGIN' in content_upper
    ) and ('COMMIT' in content_upper or 'ROLLBACK' in content_upper)
    
    if has_explicit_transaction:
        # אם יש טרנזקציה מפורשת, נריץ את כל הסקריפט כמקשה אחת
        logger.debug("Script contains explicit transaction - executing as single block")
        try:
            # ביטול auto-commit זמני
            connection.autocommit = False
            
            # חלוקת הסקריפט להצהרות ופעולה על כל אחת בנפרד
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            
            # commit ידני
            connection.commit()
            logger.info(f"Successfully executed script with explicit transaction from {script_path}")
            return True
        except mysql.connector.Error as e:
            error_msg = f"Error executing script with explicit transaction in {script_path}:\nError: {e}"
            logger.error(error_msg)
            messagebox.showerror("Error", error_msg)
            print(f"\033[91m{error_msg}\033[0m")
            connection.rollback()
            return False
        finally:
            # החזרת auto-commit למצב הרגיל
            connection.autocommit = True
    else:
        # אם אין טרנזקציה מפורשת, נריץ לפי הצהרות נפרדות
        logger.debug("Script has no explicit transaction - executing statement by statement")
        statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            try:
                logger.debug(f"Executing statement {i}/{len(statements)}: {statement[:50]}...")
                cursor.execute(statement)
                connection.commit()
            except mysql.connector.Error as e:
                error_msg = f"Error executing statement in {script_path}:\n{statement}\nError: {e}"
                logger.error(error_msg)
                messagebox.showerror("Error", error_msg)
                print(f"\033[91m{error_msg}\033[0m")
                connection.rollback()
                return False
        
        logger.info(f"Successfully executed {len(statements)} statements from {script_path}")
        return True


def process_single_script(cursor, connection, script_path, db_name):
    """מעבד סקריפט יחיד"""
    script_name = os.path.basename(script_path)
    logger.info(f"Processing script: {script_name}")
    
    if not os.path.exists(script_path):
        logger.error(f"Script {script_path} not found. Skipping.")
        print(f"Script {script_path} not found. Skipping.")
        return False

    with open(script_path, 'r', encoding='utf-8') as script_file:
        script_content = script_file.read()

    # החלפת שם מסד הנתונים בסקריפט
    script_content = replace_db_name_in_script(script_path, db_name)
    
    # ביצוע הסקריפט
    success = execute_script_statements(cursor, connection, script_path, script_content)
    
    if success:
        logger.info(f"Executed {script_name} successfully")
        print(f"\033[92mExecuted {script_name} successfully.\033[0m")
    else:
        logger.warning(f"Skipping {script_name} due to error, continuing with next script")
        print(f"\033[93mSkipping {script_name} due to error, continuing with next script.\033[0m")
         
    return success


def test_server_connection(config):
    """בודק את החיבור לשרת MySQL"""
    logger.info("Testing MySQL server connection")
    try:
        connection = mysql.connector.connect(**config)
        connection.close()
        logger.info("MySQL server connection successful")
        messagebox.showinfo("Connection Test", "Successfully connected to MySQL server!")
        return True
    except mysql.connector.Error as e:
        error_msg = f"Failed to connect to MySQL server: {e}"
        logger.error(error_msg)
        messagebox.showerror("Connection Error", error_msg)
        return False


def create_database_if_not_exists(config, db_name):
    """מתחבר לשרת MySQL ויוצר את מסד הנתונים אם הוא לא קיים"""
    logger.info(f"Checking if database '{db_name}' exists")
    
    # התחברות לשרת ללא מסד נתונים ספציפי
    server_connection = mysql.connector.connect(**config)
    cursor = server_connection.cursor()
    
    try:
        # בדיקה אם מסד הנתונים קיים
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        
        if db_name in databases:
            logger.info(f"Database '{db_name}' already exists")
            messagebox.showinfo("Database Status", f"Database '{db_name}' already exists.")
        else:
            # יצירת מסד הנתונים
            logger.info(f"Creating database '{db_name}'")
            cursor.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"Database '{db_name}' created successfully")
            messagebox.showinfo("Database Created", f"Database '{db_name}' created successfully.")
            
    except mysql.connector.Error as e:
        error_msg = f"Error creating database '{db_name}': {e}"
        logger.error(error_msg)
        messagebox.showerror("Database Error", error_msg)
        raise
    finally:
        cursor.close()
        server_connection.close()


def run_scripts_by_order(config, db_name, script_root, order_file_path):
    """פונקציה ראשית להרצת סקריפטים לפי סדר מקובץ"""
    logger.info("Starting script execution by order")
    logger.info(f"Database: {db_name}, Script root: {script_root}, Order file: {order_file_path}")
    
    # בדיקה/יצירת מסד הנתונים
    create_database_if_not_exists(config, db_name)
    
    # התחברות למסד הנתונים הספציפי
    config_with_db = config.copy()
    config_with_db['database'] = db_name
    
    connection = mysql.connector.connect(**config_with_db)
    logger.info(f"Connected to database '{db_name}' successfully")
    messagebox.showinfo("Success", f"Connected to database '{db_name}' successfully.")
   
    cursor = connection.cursor()

    try:
        # קריאת סדר ההרצה מקובץ
        logger.info(f"Reading execution order from: {order_file_path}")
        print(f"Reading execution order from: {order_file_path}")
        execution_order = parse_execution_order_file(order_file_path)
        
        if not execution_order:
            logger.warning("No scripts found in the order file")
            messagebox.showinfo("No Scripts", "No scripts found in the order file.")
            return
        
        # בדיקת קיום הקבצים
        found_scripts, missing_scripts = scan_and_validate_scripts(script_root, execution_order)
        
        # הצגת תוכנית ההרצה
        display_execution_plan(execution_order, found_scripts, missing_scripts)
        
        if missing_scripts:
            logger.warning(f"Found {len(missing_scripts)} missing scripts")
            response = messagebox.askyesno("Missing Scripts", 
                                         f"{len(missing_scripts)} scripts are missing.\nContinue with available scripts?")
            if not response:
                logger.info("User chose to cancel due to missing scripts")
                return
        
        logger.info("Starting script execution")
        print("\n=== Starting Script Execution ===")
        
        # הרצת הסקריפטים לפי הסדר
        successful_scripts = []
        failed_scripts = []
        
        for i, (folder_name, script_path) in enumerate(found_scripts, 1):
            script_name = os.path.basename(script_path)
            logger.info(f"Executing {i}/{len(found_scripts)}: [{folder_name}] {script_name}")
            print(f"Executing {i}/{len(found_scripts)}: [{folder_name}] {script_name}")
            
            success = process_single_script(cursor, connection, script_path, db_name)
            if success:
                successful_scripts.append(script_name)
            else:
                failed_scripts.append(script_name)
        
        # סיכום ההרצה
        logger.info("Execution Summary:")
        logger.info(f"Total scripts in order: {len(execution_order)}")
        logger.info(f"Scripts found: {len(found_scripts)}")
        logger.info(f"Scripts missing: {len(missing_scripts)}")
        logger.info(f"Scripts executed successfully: {len(successful_scripts)}")
        logger.info(f"Scripts failed: {len(failed_scripts)}")
        
        print(f"\n=== Execution Summary ===")
        print(f"Total scripts in order: {len(execution_order)}")
        print(f"Scripts found: {len(found_scripts)}")
        print(f"Scripts missing: {len(missing_scripts)}")
        print(f"Scripts executed successfully: {len(successful_scripts)}")
        print(f"Scripts failed: {len(failed_scripts)}")
        
        summary_message = f"Execution completed!\n\n"
        summary_message += f"📋 Total in order: {len(execution_order)}\n"
        summary_message += f"📁 Found: {len(found_scripts)}\n"
        summary_message += f"❌ Missing: {len(missing_scripts)}\n"
        summary_message += f"✅ Successful: {len(successful_scripts)}\n"
        summary_message += f"⚠️ Failed: {len(failed_scripts)}"
        
        messagebox.showinfo("Execution Summary", summary_message)
        logger.info("Script execution completed successfully")
                       
    except Exception as e:
        logger.error(f"Error during script execution: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def replace_db_name_in_script(script_path, db_name):
    """מחליף את שם מסד הנתונים בסקריפט ומתאים פקודות USE"""
    with open(script_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # החלפת שם מסד הנתונים הישן בחדש
    content = content.replace('kupathairnew', db_name)
    
    # עיבוד פקודות USE - החלפה במקום הסרה
    lines = content.split('\n')
    filtered_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        # החלפת פקודות USE במקום הסרתן
        if line_stripped.upper().startswith('USE '):
            # החלפת שם מסד הנתונים בפקודת USE
            new_use_command = f"USE `{db_name}`;"
            filtered_lines.append(new_use_command)
            logger.debug(f"Replaced USE command with: {new_use_command}")
        else:
            filtered_lines.append(line)
    
    filtered_content = '\n'.join(filtered_lines)
    logger.debug(f"Script processed: replaced USE commands, replaced db name with '{db_name}'")
    return filtered_content


def create_database_connection_config():
    """יוצר את קונפיגורציית החיבור למסד הנתונים"""
    host = host_entry.get() or DB_CONFIG['host']
    user = user_entry.get() or DB_CONFIG['user']
    password = password_entry.get() or DB_CONFIG['password']
    
    return {
        'host': host,
        'user': user,
        'password': password,
        'auth_plugin': 'caching_sha2_password'
    }


def get_user_inputs():
    """מחזיר את כל הקלטי המשתמש"""
    database = db_name_entry.get() or DB_CONFIG['database']
    script_root_folder = script_root_entry.get()
    order_file_path = order_file_entry.get()
    
    return database, script_root_folder, order_file_path


def on_run_button_click():
    """פונקציה שמופעלת כשלוחצים על כפתור ההרצה"""
    logger.info("Run button clicked")
    run_button.config(state=tk.DISABLED)
    
    try:
        config = create_database_connection_config()
        database, script_root_folder, order_file_path = get_user_inputs()
        
        logger.info(f"User inputs - Database: {database}, Script root: {script_root_folder}, Order file: {order_file_path}")
        
        if not order_file_path:
            logger.error("No execution order file path specified")
            messagebox.showerror("Error", "Please specify the execution order file path.")
            return
        
        # בדיקת חיבור לשרת תחילה
        if not test_server_connection(config):
            logger.error("Failed to connect to MySQL server")
            return
        
        run_scripts_by_order(config, database, script_root_folder, order_file_path)
        logger.info(f"Scripts execution completed successfully for database '{database}'")
        messagebox.showinfo("Success", f"Scripts execution completed for database '{database}'.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        messagebox.showerror("Error", f"An error occurred: {e}")
    finally:
        run_button.config(state=tk.NORMAL)
        logger.info("Run button re-enabled")


def on_test_connection_click():
    """פונקציה לבדיקת חיבור לשרת"""
    logger.info("Test connection button clicked")
    test_connection_button.config(state=tk.DISABLED)
    
    try:
        config = create_database_connection_config()
        test_server_connection(config)
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        messagebox.showerror("Error", f"Error testing connection: {e}")
    finally:
        test_connection_button.config(state=tk.NORMAL)


def create_gui():
    """יוצר את הממשק הגרפי"""
    global root, host_entry, user_entry, password_entry, db_name_entry, script_root_entry, order_file_entry, run_button, test_connection_button
    
    root = tk.Tk()
    root.title("MySQL Script Runner - Order Based")
    root.geometry("600x550")

    tk.Label(root, text="Enter Host:").pack(pady=5)
    host_entry = tk.Entry(root, width=50)
    host_entry.insert(0, DB_CONFIG['host'])
    host_entry.pack(pady=5)

    tk.Label(root, text="Enter User:").pack(pady=5)
    user_entry = tk.Entry(root, width=50)
    user_entry.insert(0, DB_CONFIG['user'])
    user_entry.pack(pady=5)

    tk.Label(root, text="Enter Password:").pack(pady=5)
    password_entry = tk.Entry(root, width=50, show="*")
    password_entry.insert(0, DB_CONFIG['password'])
    password_entry.pack(pady=5)

    # כפתור לבדיקת חיבור
    test_connection_button = tk.Button(root, text="Test Connection", command=on_test_connection_click, bg="lightblue", font=("Arial", 10))
    test_connection_button.pack(pady=5)

    tk.Label(root, text="Enter Database Name:").pack(pady=5)
    db_name_entry = tk.Entry(root, width=50)
    db_name_entry.insert(0, DB_CONFIG['database'])
    db_name_entry.pack(pady=5)

    tk.Label(root, text="Enter Script Root Path:").pack(pady=5)
    script_root_entry = tk.Entry(root, width=50)
    script_root_entry.insert(0, 'scripts')
    script_root_entry.pack(pady=5)

    # שדה חדש לקובץ סדר ההרצה
    tk.Label(root, text="Enter Execution Order File Path:").pack(pady=5)
    order_file_entry = tk.Entry(root, width=50)
    order_file_entry.insert(0, 'files.txt')
    order_file_entry.pack(pady=5)

    run_button = tk.Button(root, text="Run Scripts by Order", command=on_run_button_click, bg="lightgreen", font=("Arial", 12))
    run_button.pack(pady=15)
  


def main():
    """פונקציה ראשית של התוכנית"""
    logger.info("Starting MySQL Script Runner application")
    try:
        create_gui()
        logger.info("GUI created successfully")
        root.mainloop()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"Error starting application: {e}")


if __name__ == "__main__":
    main()