import re 
import pandas as pd
from db_connectors.postgressql_connector import get_car_brand_models,get_car_brand_model_ids
import logging
import datetime

logger = logging.getLogger(__name__)

def normalize(text):
    return re.sub(r'\s+', ' ', text.lower())

def auction_brand_model_enriched(auction_title, brands):
    """
    Docstring for auction_brand_model_enriched
    
    :param auction_title: string
    :param brands: list of tuples
    :return: string
    """
    found = []
    for brand in brands:
        #brand is a tuple ('brand_name',)
        pattern = brand[0]
        pattern = rf'\b{re.escape(pattern)}\b'
        auction_title = normalize(auction_title)
        if re.search(pattern, auction_title):
            found.append(brand[0])

    if len(found) == 1:
        return found[0]

    # brak marki albo konflikt
    return None

def auction_brands_enriched_output_list(auctions_dict,brands):
    """
    Docstring for auction_brands_enriched_output_list
    
    :param auctions: Description
    :param brands: Description
    """

    output_list = []
    for id, auction in auctions_dict.items():
        auction_title = auction[0]
        auction_date = auction[1]
        auction_city = auction[2]
        auction_region = auction[3]
        auction_starting_price = auction[4]
        auction_target_price = auction[5]
        auction_url = auction[6]
        brand = auction_brand_model_enriched(auction_title, brands)

        output_list.append([auction_title, auction_date, auction_city, auction_starting_price, auction_target_price, auction_url, brand])
    return output_list

def preparing_pg_auction_input_list(auctions_list):
    logger.info("Starting preparing auction list to be inserted into pg at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    output_list =[]
    for i in auctions_list:
        brand = i[6] #brand
        if brand is None:
            continue
        models = get_car_brand_models(brand)
        model = auction_brand_model_enriched(i[0],models)
        id_list = get_car_brand_model_ids(brand,model)
        if len(id_list) > 0:    
            brand_id = id_list[0][0]
            model_id = id_list[0][1]
        else:
            brand_id = None
            model_id = None
        output_list.append(i[:6] + [brand_id,model_id])
    logger.info("Prepared list of " + str(len(output_list)) + " auctions to be inserted into pg at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return output_list
