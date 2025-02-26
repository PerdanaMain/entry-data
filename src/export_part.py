from models.equipment import get_equipment_on_parts
from models.sensor_data import find_all_sensors, find_single_sensor
from models.envelope import find_envelope_by_part_id
from models.tag import find_tag_by_web_id
from models.interpolated import find_interpolated_by_tag_id
from utils.database import get_main_connection, get_collector_connection
from pathlib import Path
import pandas as pd  # type: ignore
import os


def export_to_xlsx(parts, dir, filename):
    try:
        # Create directory if it doesn't exist
        os.makedirs(dir, exist_ok=True)

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(parts)

        # save to ./public/export
        output_path = f"{dir}{filename}"
        df.to_excel(output_path, index=False)

        print(f"Data berhasil diexport ke {filename}!")
    except Exception as e:
        print(f"Error exporting to xlsx: {e}")


def interpolated():
    conn = get_main_connection()
    connn = get_collector_connection()
    dir = "public/export/interpolated/"

    current_dir = Path(__file__).parent
    excel_file = current_dir.parent / "public" / "req.xlsx"

    if not excel_file.exists():
        print(f"File tidak ditemukan di: {excel_file}")
        return

    xl = pd.ExcelFile(excel_file)
    sheet_names = xl.sheet_names

    df = pd.read_excel(excel_file, sheet_name=sheet_names[0])

    found = []
    not_found = []
    for index, row in df.iterrows():
        part = find_single_sensor(
            conn, equipment_name=row["EQUIPMENT"], part_name=row["SENSOR"]
        )
        if part is not None:
            found.append(part)
            tag = find_tag_by_web_id(connn, part["web_id"])
            values = find_interpolated_by_tag_id(connn, tag["id"])
            filename = f"{part['equipment_name']} - {part['part_name']}.xlsx"
            export_to_xlsx(values, dir, filename)
        else:
            # not_found.append(row)
            continue

    print(f"Found: {len(found)}")
    print(f"Not Found: {not_found}")

    # tag = find_tag_by_web_id(connn, part["web_id"])
    # values = find_interpolated_by_tag_id(connn, tag["id"])
    # print(len(values))


def main():
    conn = get_main_connection()
    connn = get_collector_connection()
    equipments = get_equipment_on_parts(conn=conn)
    parts = find_all_sensors(conn=conn)
    dir = "public/export/values/"

    for part in parts:
        envelopes = find_envelope_by_part_id(conn=connn, part_id=part["part_id"])
        if envelopes is not None:  # Only export if there's data
            filename = f"{part["equipment_name"]} - {part['part_name']}.xlsx"
            export_to_xlsx(envelopes, dir, filename)

    # for equipment in equipments:
    #     parts = find_sensor_data_by_equipment_id(conn, equipment["equipment_id"])
    #     if parts:  # Only export if there's data
    #         filename = f"{equipment['name']}.xlsx"
    #         export_to_xlsx(parts, filename)


if __name__ == "__main__":
    interpolated()
