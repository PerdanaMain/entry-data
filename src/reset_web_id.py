from pathlib import Path
from utils.database import get_main_connection, get_collector_connection
from models.equipment import *
from models.envelope import delete_envelope_by_part_id, find_envelope_by_part_id
from models.sensor_data import *
from models.predict import delete_predict_by_part_id
from models.feature import delete_feature_by_part_id
from main import read_excel_file

import pandas as pd


def remove_affected_fields(part_id):
    conn = get_main_connection()
    connn = get_collector_connection()
    try:
        # remove the predict
        delete_predict_by_part_id(conn, part_id)

        # remove the feature
        delete_feature_by_part_id(conn, part_id)

        # remove the envelope
        delete_envelope_by_part_id(connn, part_id)

    except Exception as e:
        print(f"Error removing affected fields: {e}")
    finally:
        conn.close()


def remove_sensor_data(part_id):
    conn = get_main_connection()
    try:
        # remove the sensor data
        delete_detail_sensor_data(conn, part_id)
        delete_sensor_data(conn, part_id)
    except Exception as e:
        print(f"Error removing sensor data: {e}")
    finally:
        conn.close()


def main():
    current_dir = Path(__file__).parent
    path = current_dir.parent / "public" / "deleted-tag.xlsx"
    sheet_name = "Sheet1"
    conn = get_main_connection()
    # part_id = "33a1ef3b-5c0d-47eb-84ab-e1c63f8e9241"
    # remove_affected_fields(part_id)

    df = read_excel_file(path, sheet_name)
    # remove first row
    df = df

    for index, row in df.iterrows():
        equipment = get_equipment_by_tag(conn, row["Unnamed: 1"])
        if equipment is not None:
            print(equipment["id"])
            sensors = find_sensor_data_by_equipment_id(conn, equipment["id"])
            for sensor in sensors:
                print(sensor)
                # remove_affected_fields(sensor["part_id"])
                # remove_sensor_data(sensor["part_id"])
        # for sensor in sensors:
        # print(sensor)


if __name__ == "__main__":
    main()
    # print("Hai")
