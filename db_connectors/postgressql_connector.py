import psycopg
import logging
import datetime
from datetime import datetime as dt
from secrets_folder.postgress_sql import connection_url

logger = logging.getLogger(__name__)

def connect_to_db():
    conn = None
    user = None
    db_name = None
    try:
        conn = psycopg.connect(
            connection_url
        )

        with conn.cursor() as cur:
            cur.execute("select current_database() db_name, current_user user;")
            a = (cur.fetchone())
            user, db_name = a
           # logger.info("Connected to posrtgress db "+ a[0] + " as " + a[1] +" at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return conn
    except psycopg.OperationalError as e:
        logger.error(f"OperationalError connecting to database: {e}", exc_info=True)
    except psycopg.DatabaseError as e:
        logger.error(f"DatabaseError occurred: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {e}", exc_info=True)
    finally:
        if conn is None:
            logger.warning("Connection was not established.")

"""
INSERT FUNCTIONS
"""
def pg_insert_car_auctions(auctions):
    connection = connect_to_db()
    logger.info("Connected to posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor = connection.cursor()
    logger.info("Starting inserting car auctions into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    failed_auctions = []
    #loading in batches and loggin after each batch
    batch_size = 100
    for idx, auction in enumerate(auctions, start=1):
        auction_title = auction[0]
        auction_date = dt.strptime(auction[1], "%d.%m.%Y").date()
        auction_city = auction[2]
        starting_price = auction[3]
        target_price = auction[4]
        auction_url = auction[5]
        car_make_id = auction[6]
        car_model_id = auction[7]
        try:
            sql = """SELECT upsert_auction(%s::text,
        %s::date,
        %s::text,
        %s::numeric,
        %s::numeric,
        %s::text,
        %s::integer,
        %s::integer)"""
            cursor.execute(sql,(
                auction_title,
                auction_date,
                auction_city,
                starting_price,
                target_price,
                auction_url,
                car_make_id,
                car_model_id
            ))
        except Exception as e:
            connection.rollback()
            failed_auctions.append((auction, str(e)))
            logger.error(f"Unexpected error inserting auction " + auction_url + " into database: {e}", exc_info=True)
        else:
            if idx % batch_size == 0:
                connection.commit()
                logger.info("Inserted "+ auction_title +" into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    connection.commit()
    logger.info("Inserted all car auctions into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    connection.close()
    #print(failed_auctions)
    if failed_auctions:
        logger.warning(f"{len(failed_auctions)} auctions failed to insert.")
    return failed_auctions

def pg_insert_car_makes(cars):
    connection = connect_to_db()
    cursor = connection.cursor()
    logger.info("Starting inserting car makes into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #for make in cars.keys():
    for make in cars:        
        check_query = cursor.execute("SELECT * FROM car_makes WHERE make_name = %s", ([make]))
        check = check_query.fetchone()
        if check is not None:
            continue
        cursor.execute("INSERT INTO car_makes (make_name) VALUES (%s)", ([make]))
        logger.info("Inserted into posrtgress db" + str(make) + " at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    connection.commit()
    logger.info("Inserted all car makes into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    connection.close()
    return None

def pg_insert_car_models(cars):
    logger.info("Starting inserting car models into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    connection = connect_to_db()
    for make,models in cars.items():
        cursor = connection.cursor()
        make_id_query = cursor.execute("SELECT make_id FROM car_makes WHERE make_name = %s", ([make]))
        make_id = make_id_query.fetchone()
        make_id = make_id[0]
        logger.info("Starting inserting car models for car_make_id " + str(make_id) + " at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        for model in models:
            check_query = cursor.execute("SELECT model_id FROM car_models WHERE make_id = %s AND model_name = %s", ([make_id, model]))
            check = check_query.fetchone()
            if check is not None:
                continue
            cursor.execute("INSERT INTO car_models (make_id, model_name) VALUES (%s, %s)", ([make_id, model]))
            logger.info("Inserted into posrtgress car_models " + str(model) + " for make " + str(make) + " at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        connection.commit()
    connection.close()
    logger.info("Inserted all car models into posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return None

""""
RETRIEVE FUNCTIONS
"""
def get_cars_brands():
    #getting saved makes in the postgres
    logger.info("Getting all car makes from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = connect_to_db()
    cur = con.cursor()
    sql = "SELECT make_name FROM car_makes"
    cur.execute(sql)
    makes = cur.fetchall()
    logger.info("Got all " + str(len(makes)) + " car makes from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return makes

def get_cars_models():
    #getting saved models in the postgres
    logger.info("Getting all car models from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = connect_to_db()
    cur = con.cursor()
    sql = "SELECT model_name FROM car_models"
    cur.execute(sql)
    models = cur.fetchall()
    logger.info("Got all " + str(len(models)) + " car models from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return models

def get_car_brand_models(brand):
    #getting saved models for specific brand in the postgres
    logger.info("Getting all models for brand " + str(brand) + " from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = connect_to_db()
    cur = con.cursor()
    sql = "SELECT model_name from car_models t1 inner join car_makes t2 on t1.make_id = t2.make_id where t2.make_name = %s"
    cur.execute(sql,(brand,))
    models = cur.fetchall()
    logger.info("Get all " + str(len(models)) + " models for brand " + str(brand) + " from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return models

def get_car_brand_model_ids(brand,model):

    """
    Retrieves car make id and model id for given brand and model from PostgreSQL database
     
    :param brand: brand name
    :param model: model name
    :return: list of tuples containing make_id and model_id
    """

    
    con = connect_to_db()
    cur = con.cursor()
    logger.info("Getting ids for brand " + str(brand) + " and model " + str(model) + " from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    sql = "SELECT t1.make_id,t1.model_id from car_models t1 inner join car_makes t2 on t1.make_id = t2.make_id where t2.make_name = %s and t1.model_name = %s"
    cur.execute(sql,(brand,model))
    car_ids = cur.fetchall()
    logger.info("IDs " + str(car_ids) + " for brand " + str(brand) + " and model " + str(model) + " from posrtgress db at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return car_ids