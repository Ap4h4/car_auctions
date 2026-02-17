from db_connectors.postgressql_connector import connect_to_db

def search_cars(filters: dict):
    conditions = []
    values = []

    if filters["model_name"]:
        conditions.append("model_name ILIKE %s")
        values.append(f"%{filters['model_name']}%")

    if filters["make_name"]:
        conditions.append("make_name = %s")
        values.append(filters["make_name"])

    if filters["made_year"]:
        conditions.append("made_year = %s")
        values.append(int(filters["made_year"]))



    if filters["auction_city"]:
        conditions.append("auction_city = %s")
        values.append(filters["auction_city"])

    if filters["auction_date"]:
        conditions.append("auction_date = %s")
        values.append(filters["auction_date"])

    where_clause = ""
    if conditions:
        where_clause = "WHERE t1.version_key = 1 AND   " + " AND ".join(conditions)

    query = f"""
        select t1.auction_title, cm.make_name , cm2.model_name , auction_date , auction_city , auction_url , made_year, plate_number , vin_number,
        t1.starting_price, t1.target_price 
        from auctions t1
        left join car_makes cm on t1.car_make_id = cm.make_id 
        left join car_models cm2 on t1.car_model_id = cm2.model_id 
        {where_clause}
    """

    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(query, values)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return result
