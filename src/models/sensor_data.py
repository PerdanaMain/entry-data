from psycopg2.extras import execute_batch
from datetime import datetime
import uuid


def find_sensor_data_by_part_name(conn, part_name, equipment_id):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM pf_parts WHERE part_name = %s AND equipment_id = %s",
                (part_name, equipment_id),
            )
            return cur.fetchone()
    except Exception as e:
        print(f"Error finding sensor data by part name: {e}")
        return None


def find_sensor_data_by_equipment_id(conn, equipment_id):
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    mem.name as equipment_name,
                    mem.location_tag as equipment_tag,
                    pp.part_name,
                    pp.location_tag as sensor_tag
                FROM pf_parts pp 
                JOIN ms_equipment_master mem ON pp.equipment_id = mem.id
                WHERE equipment_id = %s
            """

            cur.execute(sql, (equipment_id,))
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()
            return [dict(zip(columns, row)) for row in result]

    except Exception as e:
        print(f"Error finding sensor data by part name: {e}")
        return None


def insert_sensor_to_feature(conn, part_id, features_id, value, date_time):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dl_features_data (id,features_id, part_id, date_time, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    str(uuid.uuid4()),
                    features_id,
                    part_id,
                    date_time,
                    value,
                    datetime.now(),
                    datetime.now(),
                ),
            )
            conn.commit()
    except Exception as e:
        print(f"Error inserting sensor to feature: {e}")


def insert_detail_sensor_data(
    conn,
    part_id,
    upper_threshold,
    lower_threshold,
    predict_status,
    time_failure,
    predict_value,
    one_hundred_percent_condition,
    percent_condition,
):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pf_details (id, part_id, upper_threshold, lower_threshold, predict_status, time_failure, predict_value, one_hundred_percent_condition, percent_condition, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(uuid.uuid4()),
                    part_id,
                    upper_threshold,
                    lower_threshold,
                    predict_status,
                    time_failure,
                    predict_value,
                    one_hundred_percent_condition,
                    percent_condition,
                    datetime.now(),
                    datetime.now(),
                ),
            )
            conn.commit()
    except Exception as e:
        print(f"Error inserting detail sensor data: {e}")


def insert_sensor_data(
    conn, equipment_id, part_name, type_id, location_tag="NON DCS", web_id=None
):
    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO pf_parts (
                    id, equipment_id, web_id, part_name, created_at, updated_at, type_id, location_tag
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            cur.execute(
                insert_query,
                (
                    str(uuid.uuid4()),
                    equipment_id,
                    web_id,
                    part_name,
                    datetime.now(),
                    datetime.now(),
                    None,
                    location_tag,
                ),
            )

            conn.commit()
            print(f"\nData {part_name} berhasil disimpan ke database!")

    except Exception as e:
        conn.rollback()
        print(f"Terjadi kesalahan saat menyimpan data: {str(e)}")
        raise
