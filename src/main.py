import pandas as pd  # type: ignore
from pathlib import Path
from utils.database import get_main_connection
from models.sensor_data import (
    insert_sensor_data,
    find_sensor_data_by_part_name,
    insert_sensor_to_feature,
    insert_detail_sensor_data,
)
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


def execute(df, features_id, sheet_name):
    try:
        # Setup database connection
        print("Menghubungkan ke database...")
        conn = get_main_connection()

        equipment = find_equipment_by_tag_location(conn, location_tag=sheet_name)

        # Buat kelompok sensor
        sensor_groups, names = create_sensor_groups(df)
        for name in names:
            old_name = name
            name = name.replace("_", " ")
            name = name.upper()

            insert_sensor_data(
                conn=conn,
                equipment_id=equipment[0],
                part_name=name,
                type_id=None,
                location_tag="NON DCS",
            )

            part = find_sensor_data_by_part_name(conn, name, equipment[0])
            feature = sensor_groups[old_name].iloc[0]
            one_hundred_percent_condition = float(feature["Normal Value"])
            upper_threshold = float(feature["Unnamed: 5"])
            lower_threshold = float(feature["Unnamed: 6"])

            insert_detail_sensor_data(
                conn=conn,
                part_id=part[0],
                upper_threshold=upper_threshold,
                lower_threshold=lower_threshold,
                one_hundred_percent_condition=one_hundred_percent_condition,
                percent_condition=None,
                time_failure=None,
                predict_status=None,
                predict_value=None,
            )

        for name in names:
            old_name = name
            name = name.replace("_", " ")
            name = name.upper()
            part = find_sensor_data_by_part_name(conn, name, equipment[0])

            for index, feature in sensor_groups[old_name].iterrows():
                value = (
                    feature["Vibration Value"]
                    if feature["Vibration Value"] != "-"
                    else 0
                )

                insert_sensor_to_feature(
                    conn=conn,
                    part_id=part[0],
                    features_id=features_id,
                    value=value,
                    date_time=feature["Date"],
                )
    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")


def main():
    # Baca file Excel
    current_dir = Path(__file__).parent
    excel_file = current_dir.parent / "public" / "nondcs.xlsx"
    features_id = "9dcb7e40-ada7-43eb-baf4-2ed584233de7"

    if not excel_file.exists():
        print(f"File tidak ditemukan di: {excel_file}")
        return

    xl = pd.ExcelFile(excel_file)
    sheet_names = xl.sheet_names

    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        if df is None:
            print(f"Melewati sheet {sheet_name} karena error")
            continue
        execute(df, features_id, sheet_name)


if __name__ == "__main__":
    # main()
    print("Hai")
