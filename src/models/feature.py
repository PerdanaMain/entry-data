def find_feature_by_part_id(conn, part_id):
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT * 
            FROM dl_features_data df 
            WHERE df.part_id = %s
            """

            cur.execute(
                sql,
                (part_id,),
            )
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()
            return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        print(f"Error finding feature by part id: {e}")
        return None


def delete_feature_by_part_id(conn, part_id):
    try:
        with conn.cursor() as cur:
            sql = """
            DELETE
            FROM dl_features_data df 
            WHERE df.part_id = %s
            """

            cur.execute(
                sql,
                (part_id,),
            )
            conn.commit()
            print(f"Deleted feature by part id: {part_id}")
    except Exception as e:
        print(f"Error deleting feature by part id: {e}")
