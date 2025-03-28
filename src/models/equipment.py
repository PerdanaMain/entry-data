def find_equipment_by_tag_location(conn, location_tag):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM ms_equipment_master WHERE location_tag = %s",
                (location_tag,),
            )
            return cur.fetchone()
    except Exception as e:
        print(f"Error finding equipment by tag location: {e}")
        return None


def get_equipment_on_parts(conn):
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT 
                DISTINCT pp.equipment_id as equipment_id,
                mem.name 
            FROM pf_parts pp
            JOIN ms_equipment_master mem ON pp.equipment_id = mem.id
            """

            cur.execute(sql)
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()

            return [dict(zip(columns, row)) for row in result]

    except Exception as e:
        print(f"Error getting equipment on parts: {e}")
        return None


def get_equipment_by_tag_and_name(conn, tag_location, equipment_name):
    try:
        with conn.cursor() as cur:
            like_name = f"%{equipment_name}%"

            sql = """
            SELECT *
            FROM ms_equipment_master
            WHERE location_tag = %s OR name like %s
            """

            cur.execute(
                sql,
                (tag_location, like_name),
            )
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()
            result = dict(zip(columns, result[0]))

            return result
    except Exception as e:
        print(f"Error getting equipment by tag name: {e}")
        return None


def get_equipment_by_tag(conn, tag_location):
    try:
        with conn.cursor() as cur:
            sql = """
            SELECT *
            FROM ms_equipment_master
            WHERE location_tag = %s
            """

            cur.execute(
                sql,
                (tag_location,),
            )
            columns = [col[0] for col in cur.description]
            result = cur.fetchall()
            result = dict(zip(columns, result[0]))

            return result
    except Exception as e:
        print(f"Error getting equipment by tag name: {e}")
        return None
