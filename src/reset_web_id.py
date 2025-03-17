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
        delete_predict_by_part_id(conn, part_id)

        # remove the feature
        delete_feature_by_part_id(conn, part_id)

        # remove the envelope
        delete_envelope_by_part_id(connn, part_id)

    except Exception as e:
        print(f"Error removing affected fields: {e}")
    finally:
        conn.close()


def main():
    current_dir = Path(__file__).parent
    path = current_dir.parent / "public" / "r.xlsx"
    sheet_name = "Reset"
    conn = get_main_connection()
    part_id = "215c035b-bea8-4c70-94e5-210e6bbe3a5f"
    remove_affected_fields(part_id)

    # web_db = "F1DPw1kUu10ziUaXEx2rIyo4pADA8AAAS1RKQi1LSTAwLVBJMVxUSkIzLlBVTFZFUklaRVIgRiBMVUIgT0lMIFBSRVNT"
    # web_xlsx = "F1DPw1kUu10ziUaXEx2rIyo4pADQ8AAAS1RKQi1LSTAwLVBJMVxUSkIzLlBVTFZFUklaRVIgRiBMVUIgT0lMIFRFTVA"

    # try:
    #     res = web_db == web_xlsx
    #     print(res)
    #     if res != True:
    #         print("Web ID tidak sama")
    #         remove_affected_fields(part_id)
    #     else:
    #         print("Web ID sama")

    # except Exception as e:
    # print(f"Terjadi kesalahan: {str(e)}")


if __name__ == "__main__":
    main()
    # print("Hai")
