from scrapping.scrapping import debt_auctions_scrapper, get_raw_cars_brand_models,get_raw_cars_brands
from scrapping.sparql_wikidata import fetch_wikidata, build_dictionary
from db_connectors.mongo_db_connector import mongo_bulk_upload,mongo_truncate_collection, connect_to_db as mongo_connect_to_db,get_all_auctions
from db_connectors.postgressql_connector import connect_to_db as pg_connect_to_db, pg_insert_car_makes, pg_insert_car_models,pg_insert_car_auctions, get_cars_brands, get_car_brand_models,get_car_brand_model_ids
from config.logging_config import setup_logging
from etl.debt_auctions_processing import auction_brands_enriched_output_list, auction_brand_model_enriched, preparing_pg_auction_input_list

import pandas as pd






def main():
    setup_logging()
    """
    #SCRAPPING AUCTIONS AND LOADING INTO MONGO
    """
    df = debt_auctions_scrapper() #tested
    mongo_truncate_collection("auctions") #tested 
    mongo_bulk_upload("auctions", df) #tested
    
    
    """
    #SCRAPPING WIKIDATA FOR ALL CARS AND INSERTING INTO PG DB
    """
    """
    wiki_data = fetch_wikidata() #scrapping wiki data
    wiki_dict = build_dictionary(wiki_data) #building dictionary
    pg_insert_car_makes(wiki_dict) 
    pg_insert_car_models(wiki_dict)
    """
    
    """
    SCRAPPING OTOMOTO DATA AND SAVING TO CSV - TESTING AND TBC WITH POSTGRESQL
    """
    """
    #scrapping all brands from otomoto
    #makes = get_raw_cars_brands()
    #getting list of brands already inserted into postgres
    con = pg_connect_to_db()
    cur = con.cursor()
    sql = "SELECT make_name FROM car_makes"
    cur.execute(sql)
    makes = cur.fetchall()
    
    #scrapping models for given list of brands
    for make in makes:
        dict_models = get_raw_cars_brand_models(make[0])
        pg_insert_car_models(dict_models)

    """
#ZROB GITAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
    """
    ENRICHING AUCTIONS AND LOADING INTO POSTGRESQL
    """
    #getting saved auctions in the mongo
    auctions_dict = get_all_auctions()
    car_all_brands = get_cars_brands()
    output_brand_list = auction_brands_enriched_output_list(auctions_dict,car_all_brands)
    output_list = preparing_pg_auction_input_list (output_brand_list)
    pg_insert_car_auctions(output_list)



if __name__ == "__main__":
    main()
