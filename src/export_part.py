from models.equipment import get_equipment_on_parts
from models.sensor_data import find_sensor_data_by_equipment_id
from utils.database import get_main_connection
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
    equipments = get_equipment_on_parts(conn=conn)

    for equipment in equipments:
        parts = find_sensor_data_by_equipment_id(conn, equipment["equipment_id"])
        if parts:  # Only export if there's data
            filename = f"{equipment['name']}.xlsx"
            export_to_xlsx(parts, filename)


if __name__ == "__main__":
    main()
