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
