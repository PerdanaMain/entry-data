import pandas as pd
from pathlib import Path
from utils.database import get_main_connection
from models.sensor_data import insert_sensor_data
from models.equipment import find_equipment_by_tag_location


def read_excel_file(file_path, sheet_name):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
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
    names = []

    for point in measurement_points:
        for direction in directions:
            try:
                group_name = f"{point.lower().replace(' ', '_')}_{direction.lower()}"
                sensor_groups[group_name] = df[
                    (df["Measurement Point"] == point) & (df["Directions"] == direction)
                ]
                names.append(group_name)
            except Exception as e:
                print(f"Terjadi kesalahan: {str(e)}")
                continue
    return sensor_groups, names


def main():
    try:
        # Baca file Excel
        current_dir = Path(__file__).parent
        excel_file = current_dir.parent / "public" / "nondcs.xlsx"
        sheet_name = "3FW-P020B"

        if not excel_file.exists():
            print(f"File tidak ditemukan di: {excel_file}")
            return

        df = read_excel_file(excel_file, sheet_name)
        if df is None:
            return

        # Setup database connection
        print("Menghubungkan ke database...")
        conn = get_main_connection()

        equipment = find_equipment_by_tag_location(conn, location_tag=sheet_name)

        # Buat kelompok sensor
        sensor_groups, names = create_sensor_groups(df)
        for name in names:
            name = name.replace("_", " ")
            name = name.upper()

            insert_sensor_data(
                conn=conn,
                equipment_id=equipment[0],
                part_name=name,
                type_id=None,
                location_tag="NON DCS",
            )

    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")


if __name__ == "__main__":
    main()
