from psycopg2.extras import execute_batch
import uuid


def insert_sensor_data(conn, sensor_groups):
    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO sensor_data (
                    date, measurement_point, directions, 
                    vibration_value, normal_value, unnamed_5, unnamed_6
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            total_records = 0

            for group_name, group_df in sensor_groups.items():
                print(f"\nMenyimpan data untuk {group_name}...")

                # Konversi DataFrame ke list of tuples untuk batch insert
                data = [
                    (
                        str(uuid.uuid4()),
                        row["Date"],
                        row["Measurement Point"],
                        row["Directions"],
                        row["Vibration Value"],
                        row["Normal Value"],
                        row["Unnamed: 5"],
                        row["Unnamed: 6"],
                    )
                    for _, row in group_df.iterrows()
                ]

                # Batch insert dengan page size 1000
                execute_batch(cur, insert_query, data, page_size=1000)

                group_records = len(data)
                total_records += group_records
                print(f"Data {group_name} berhasil disimpan! ({group_records} records)")

            conn.commit()
            print(f"\nTotal {total_records} records berhasil disimpan ke database!")

    except Exception as e:
        conn.rollback()
        print(f"Terjadi kesalahan saat menyimpan data: {str(e)}")
        raise
