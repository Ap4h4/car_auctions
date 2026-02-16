import logging
import datetime
from scrapping.scrapping import debt_auctions_scrapper, get_raw_cars_brand_models,get_raw_cars_brands
from scrapping.sparql_wikidata import fetch_wikidata, build_dictionary
from db_connectors.mongo_db_connector import mongo_bulk_upload,mongo_truncate_collection, connect_to_db as mongo_connect_to_db,get_all_auctions
from db_connectors.postgressql_connector import connect_to_db as pg_connect_to_db, pg_insert_car_makes, pg_insert_car_models,pg_insert_car_auctions, get_cars_brands, get_car_brand_models,get_car_brand_model_ids
from config.logging_config import setup_logging
from etl.debt_auctions_processing import auction_brands_enriched_output_list, auction_brand_model_enriched, preparing_pg_auction_input_list,otomoto_auctions

import pandas as pd
import os

logger = logging.getLogger(__name__)
def run_auctions_scrapping():
    """
    #SCRAPPING AUCTIONS AND LOADING INTO MONGO
    """
    logger.info("Calling auction scrapping function at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    df = debt_auctions_scrapper()
    mongo_truncate_collection("auctions")
    mongo_bulk_upload("auctions", df)
    logger.info("Completing auction scrapping function at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def run_otomoto_scrapping():
    """
    SCRAPPING OTOMOTO DATA AND SAVING TO CSV - TESTING AND TBC WITH POSTGRESQL
    """
    logger.info("Calling otomoto scrapping function at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #full dump once per month, otherwise from postgres
    if datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") == 1:
        makes = get_raw_cars_brands()
    else:
        con = pg_connect_to_db()
        cur = con.cursor()
        sql = "SELECT make_name FROM car_makes"
        cur.execute(sql)
        makes = cur.fetchall()    
    
  #scrapping models for given list of brands
    for make in makes:
        dict_models = get_raw_cars_brand_models(make[0])
        pg_insert_car_models(dict_models)
    logger.info("Completing otomoto scrapping function at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def run_auctions_enrichment():
    """
    ENRICHING AUCTIONS AND LOADING INTO POSTGRESQL
    """
    logger.info("Starting auction enrichment function")
    #getting saved auctions in the mongo
    auctions_dict = get_all_auctions()
    car_all_brands = get_cars_brands()
    output_brand_list = auction_brands_enriched_output_list(auctions_dict,car_all_brands)
    output_list = preparing_pg_auction_input_list (output_brand_list)
    pg_insert_car_auctions(output_list)
    
    #getting otomoto auctions statistics for debt auctions
    #otomoto_auctions() --time consuming, probably better to move into other step
    logger.info("Completing auction enrichment function")




def main():
    setup_logging()
    mode = os.getenv("ETL_MODE", "FULL").upper()
    logger.info(f"ETL started in {mode} mode")

    if mode in ("FULL", "SCRAPE"):
        run_auctions_scrapping()

    if mode in ("FULL", "OTOMOTO"):
        run_otomoto_scrapping()

    if mode in ("FULL", "ENRICH"):
        run_auctions_enrichment()

    logger.info("ETL finished successfully at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    main()
