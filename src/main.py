import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from utils.database import get_db_connection
from models.sensor_data import insert_sensor_data

# Load environment variables
load_dotenv()


def read_excel_file(file_path):
    try:
        df = pd.read_excel(file_path)
        print("\nInformasi Dataset:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Jumlah kolom: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")
        return None


def create_sensor_groups(df):
    measurement_points = df["Measurement Point"].unique()
    directions = df["Directions"].unique()
    sensor_groups = {}

    for point in measurement_points:
        for direction in directions:
            group_name = f"{point.lower().replace(' ', '_')}_{direction.lower()}"
            sensor_groups[group_name] = df[
                (df["Measurement Point"] == point) & (df["Directions"] == direction)
            ]

    return sensor_groups


def main():
    try:
        # Baca file Excel
        current_dir = Path(__file__).parent
        excel_file = current_dir.parent / "public" / "nondcs.xlsx"

        if not excel_file.exists():
            print(f"File tidak ditemukan di: {excel_file}")
            return

        df = read_excel_file(excel_file)
        if df is None:
            return

        # Buat kelompok sensor
        sensor_groups = create_sensor_groups(df)

        # Setup database connection
        print("Menghubungkan ke database...")
        conn = get_db_connection()

        # Insert data
        insert_sensor_data(conn, sensor_groups)

    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")
    finally:
        if "conn" in locals():
            conn.close()
            print("Koneksi database ditutup")


if __name__ == "__main__":
    main()
