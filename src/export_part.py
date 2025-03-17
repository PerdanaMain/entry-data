from models.equipment import get_equipment_on_parts
from models.sensor_data import find_all_sensors, find_single_sensor, find_sensor_by_id
from models.envelope import find_envelope_by_part_id
from models.tag import find_tag_by_web_id
from models.interpolated import find_interpolated_by_tag_id
from utils.database import get_main_connection, get_collector_connection
from pathlib import Path
from datetime import datetime
import pandas as pd  # type: ignore
import os
import re


def sanitize_filename(filename):
    """
    Convert a string to a safe filename by removing or replacing characters
    that aren't allowed in filenames.
    """
    # Replace slashes, backslashes with underscores
    safe_name = re.sub(r"[/\\]", "_", filename)

    # Replace other potentially problematic characters
    safe_name = re.sub(r'[<>:"|?*]', "", safe_name)

    # Optional: Remove leading/trailing whitespace
    safe_name = safe_name.strip()

    return safe_name


def export_to_xlsx(parts, dir, filename):
    try:
        # Create base directory if it doesn't exist
        os.makedirs(dir, exist_ok=True)

        # Sanitize the filename
        safe_filename = sanitize_filename(filename)

        # Create output path
        output_path = os.path.join(dir, safe_filename)

        # Convert to DataFrame and save
        df = pd.DataFrame(parts)
        df.to_excel(output_path, index=False)

        print(f"Data berhasil diexport ke {output_path}!")
        return True
    except Exception as e:
        print(f"Error exporting to xlsx: {e}")
        import traceback

        traceback.print_exc()
        return False


def interpolated():
    conn = get_main_connection()
    connn = get_collector_connection()
    dir = "public/export/new"  # Removed trailing slash

    found = []
    not_found = []

    part = find_sensor_by_id(conn=conn, part_id="8d1cffd2-8d46-4b50-ad8d-48152abf4d5c")
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Format date for filename

    if part is not None:
        found.append(part)
        tag = find_tag_by_web_id(connn, part["web_id"])
        values = find_interpolated_by_tag_id(connn, tag["id"])

        filename = f"{part['equipment_name']} - {part['part_name']} - {part['tag_sensor']}.xlsx"
        export_to_xlsx(values, dir, filename)
    else:
        not_found.append("Part with ID eab413a3-9a82-4fa6-9280-0d80ea320a38")

    print(f"Found: {len(found)}")
    print(f"Not Found: {not_found}")


def main():
    conn = get_main_connection()
    connn = get_collector_connection()
    parts = find_all_sensors(conn=conn)
    dir = "public/export/values"  # Removed trailing slash

    for part in parts:
        envelopes = find_envelope_by_part_id(conn=connn, part_id=part["part_id"])
        if envelopes is not None:  # Only export if there's data
            filename = f"{part['equipment_name']} - {part['part_name']}.xlsx"
            export_to_xlsx(envelopes, dir, filename)


if __name__ == "__main__":
    interpolated()
    # main()
