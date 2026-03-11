from db_connectors.postgressql_connector import connect_to_db, get_debt_car_auctions_details
import logging

logger = logging.getLogger(__name__)

def get_all_cars_brands():
    results = get_debt_car_auctions_details
    return results

def get_all_auctions_details(make=None, model=None, year=None, city=None, date=None):
    sql= """
            select * from search_auctions(%s,%s,%s,%s,%s);
            """
    db = connect_to_db()
    cur = db.cursor()
    cur.execute(sql, (make, model, year, city, date))
    results = cur.fetchall()
    return results