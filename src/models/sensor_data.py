from psycopg2.extras import execute_batch
from datetime import datetime
import uuid


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

            print(
                str(uuid.uuid4()),
                equipment_id,
                part_name,
                datetime.now(),
                datetime.now(),
                type_id,
                location_tag,
                web_id,
            )

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
            print(f"\nData part berhasil disimpan ke database!")

    except Exception as e:
        conn.rollback()
        print(f"Terjadi kesalahan saat menyimpan data: {str(e)}")
        raise
