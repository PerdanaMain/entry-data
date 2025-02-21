from pathlib import Path
from utils.database import get_main_connection, get_collector_connection
from models.equipment import get_equipment_by_tag_and_name
from models.envelope import delete_envelope_by_part_id, find_envelope_by_part_id
from models.sensor_data import find_sensor_by_equipment, update_web_id_sensor_data
from models.predict import delete_predict_by_part_id
from models.feature import delete_feature_by_part_id

import pandas as pd


def remove_affected_fields(part_id):
    conn = get_main_connection()
    connn = get_collector_connection()
    try:
        # remove the predict
        # delete_predict_by_part_id(conn, part_id)

        # remove the feature
        # delete_feature_by_part_id(conn, part_id)

        # remove the envelope
        # delete_envelope_by_part_id(conn, part_id)
        envelopes = find_envelope_by_part_id(connn, part_id)
        print(len(envelopes))

    except Exception as e:
        print(f"Error removing affected fields: {e}")
    finally:
        conn.close()


def main():
    current_dir = Path(__file__).parent
    path = current_dir.parent / "public" / "r.xlsx"
    sheet_name = "Reset"
    conn = get_main_connection()

    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
        print("\nInformasi Dataset:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Jumlah kolom: {len(df.columns)}")

        find = df.iloc[4]
        print("\nData yang dicari:")
        print(find)

        equipment = get_equipment_by_tag_and_name(
            conn=conn,
            tag_location=find["TAG LOCATION"],
            equipment_name=find["EQUIPMENT"],
        )
        print(equipment)
        part = find_sensor_by_equipment(
            conn=conn,
            equipment_id=equipment["id"],
            part_name=find["SENSOR"],
        )
        print(part)

        # remove_affected_fields(part["part_id"])
        # update_web_id_sensor_data(conn, part["part_id"], find["Web_id"])

        # res = part["web_id"] == find["Web_id"]

        # print(res)

        # loop through the data
        # for index, row in df.iterrows():
        #     print(row["Web_id"])

    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")


if __name__ == "__main__":
    main()
