def find_interpolated_by_tag_id(conn, tag_id):
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    interpolated.value,
                    interpolated.time_stamp
                FROM dl_value_tag_interpolated_temp interpolated
                WHERE interpolated.tag_id = %s
                ORDER BY interpolated.time_stamp ASC
                """

            cur.execute(sql, (tag_id,))
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()

            return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        print(f"Error finding interpolated by tag id: {e}")
        return None
