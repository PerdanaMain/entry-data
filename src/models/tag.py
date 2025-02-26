def find_tag_by_web_id(conn, web_id):
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    tag.*
                FROM dl_ms_tag tag
                WHERE tag.web_id = %s
                """

            cur.execute(sql, (web_id,))
            columns = [col[0] for col in cur.description]
            result = cur.fetchone()
            return dict(zip(columns, result))
    except Exception as e:
        print(f"Error finding tag by web id: {e}")
        return None
