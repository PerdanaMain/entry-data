from models.sensor_data import (
    find_sensor_non_dcs,
    delete_detail_sensor_data,
    delete_non_dcs_sensor_data,
)
from models.predict import delete_predict_by_part_id, find_predict_by_part_id
from models.feature import find_feature_by_part_id, delete_feature_by_part_id

from utils.database import get_main_connection


def main():
    conn = get_main_connection()
    parts = find_sensor_non_dcs(conn=conn)

    # for part in parts:
    # res = find_predict_by_part_id(conn=conn, part_id=part["id"])
    # delete_predict_by_part_id(conn=conn, part_id=part["id"])

    # res = find_feature_by_part_id(conn=conn, part_id=part["id"])
    # print(len(res))
    # delete_feature_by_part_id(conn=conn, part_id=part["id"])

    # delete_detail_sensor_data(conn=conn, part_id=part["id"])

    # delete_non_dcs_sensor_data(conn=conn, part_id=part["id"])


if __name__ == "__main__":
    main()
