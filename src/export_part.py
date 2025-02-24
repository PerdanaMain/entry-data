from models.equipment import get_equipment_on_parts
from models.sensor_data import find_all_sensors
from models.envelope import find_envelope_by_part_id
from utils.database import get_main_connection, get_collector_connection
import pandas as pd
import os


def export_to_xlsx(parts, filename):
    try:
        # Create directory if it doesn't exist
        os.makedirs("public/export", exist_ok=True)

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(parts)

        # save to ./public/export
        output_path = f"public/export/{filename}"
        df.to_excel(output_path, index=False)

        print(f"Data berhasil diexport ke {filename}!")
    except Exception as e:
        print(f"Error exporting to xlsx: {e}")


def main():
    conn = get_main_connection()
    connn = get_collector_connection()
    equipments = get_equipment_on_parts(conn=conn)
    parts = find_all_sensors(conn=conn)

    for part in parts:
        envelopes = find_envelope_by_part_id(conn=connn, part_id=part["part_id"])
        if envelopes is not None:  # Only export if there's data
            filename = f"{part["equipment_name"]} - {part['part_name']}.xlsx"
            export_to_xlsx(envelopes, filename)

    # for equipment in equipments:
    #     parts = find_sensor_data_by_equipment_id(conn, equipment["equipment_id"])
    #     if parts:  # Only export if there's data
    #         filename = f"{equipment['name']}.xlsx"
    #         export_to_xlsx(parts, filename)


if __name__ == "__main__":
    main()
