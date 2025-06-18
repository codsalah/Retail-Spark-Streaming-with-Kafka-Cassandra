import subprocess
import os
import sys
from datetime import datetime

def run_duckdb_export(table_name, output_path, db_path="/tmp/dbt_db/retail_analytics.duckdb"):
    """Export a table to CSV using DuckDB"""
    try:
        # Create the export query
        query = f"COPY (SELECT * FROM {table_name}) TO '{output_path}' (HEADER, DELIMITER ',');"
        
        cmd = ["/opt/airflow/dbt/duckdb", db_path, "-c", query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print(f"Exported {table_name} to {output_path}")
            return True
        else:
            print(f"Failed to export {table_name}: {result.stderr}")
            return False
    except Exception as e:
        print(f"Exception exporting {table_name}: {e}")
        return False

def get_table_info(table_name, db_path="/tmp/dbt_db/retail_analytics.duckdb"):
    """Get information about a table"""
    try:
        query = f"SELECT COUNT(*) as count FROM {table_name};"
        cmd = ["/opt/airflow/dbt/duckdb", db_path, "-c", query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            # Extract count from output
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip().isdigit():
                    return int(line.strip())
                elif '│' in line and line.strip().replace('│', '').strip().isdigit():
                    return int(line.strip().replace('│', '').strip())
        return 0
    except Exception as e:
        print(f"Could not get info for {table_name}: {e}")
        return 0

def export_gold_data():
    """Main function to export all gold data"""
    print("Starting Gold Data Export...")
    print("=" * 50)

    # Define the tables to export
    tables = {
        'dim_customers': 'Customer dimension table with purchase metrics',
        'fct_purchases': 'Purchase fact table with transaction details',
        'daily_sales_summary': 'Daily aggregated sales metrics'
    }

    # Create timestamp for this export
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create gold directory structure
    gold_dir = "/tmp/gold_export"
    os.makedirs(gold_dir, exist_ok=True)

    # Create timestamped subdirectory
    export_dir = f"{gold_dir}/{timestamp}"
    os.makedirs(export_dir, exist_ok=True)

    print(f"Export directory: {export_dir}")
    print()
    
    exported_files = []
    total_records = 0
    
    # Export each table
    for table_name, description in tables.items():
        print(f"Exporting {table_name}...")
        print(f"   Description: {description}")

        # Get table info
        record_count = get_table_info(table_name)
        print(f"   Records: {record_count:,}")

        # Define output path
        output_path = f"{export_dir}/{table_name}.csv"

        # Export the table
        if run_duckdb_export(table_name, output_path):
            exported_files.append({
                'table': table_name,
                'file': output_path,
                'records': record_count,
                'description': description
            })
            total_records += record_count

        print()
    
    # Create a latest symlink
    latest_dir = f"{gold_dir}/latest"
    if os.path.exists(latest_dir):
        os.remove(latest_dir)
    os.symlink(timestamp, latest_dir)
    
    # Create summary file
    summary_file = f"{export_dir}/export_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("Gold Data Export Summary\n")
        f.write("=" * 30 + "\n")
        f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Export Directory: {export_dir}\n")
        f.write(f"Total Tables: {len(exported_files)}\n")
        f.write(f"Total Records: {total_records:,}\n")
        f.write("\n")
        
        f.write("Exported Tables:\n")
        f.write("-" * 20 + "\n")
        for file_info in exported_files:
            f.write(f"• {file_info['table']}.csv ({file_info['records']:,} records)\n")
            f.write(f"  {file_info['description']}\n")
            f.write("\n")
    
    # Show final summary
    print("=" * 50)
    print("Gold Data Export Completed!")
    print()
    print("Export Summary:")
    print(f"   • Export Directory: {export_dir}")
    print(f"   • Latest Link: {latest_dir}")
    print(f"   • Total Tables: {len(exported_files)}")
    print(f"   • Total Records: {total_records:,}")
    print()

    print("Exported Files:")
    for file_info in exported_files:
        file_size = "Unknown"
        try:
            size_bytes = os.path.getsize(file_info['file'])
            if size_bytes < 1024:
                file_size = f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                file_size = f"{size_bytes / 1024:.1f} KB"
            else:
                file_size = f"{size_bytes / (1024 * 1024):.1f} MB"
        except:
            pass

        print(f"   {file_info['table']}.csv ({file_info['records']:,} records, {file_size})")

    print()
    print("Access Methods:")
    print(f"   • Direct path: {export_dir}/")
    print(f"   • Latest link: {latest_dir}/")
    print(f"   • Summary file: {summary_file}")
    
    return exported_files

if __name__ == "__main__":
    try:
        export_gold_data()
    except KeyboardInterrupt:
        print("\nExport interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nExport failed: {e}")
        sys.exit(1)
