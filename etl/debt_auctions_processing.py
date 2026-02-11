import re 
import pandas as pd
from db_connectors.postgressql_connector import get_car_brand_models,get_car_brand_model_ids,get_debt_car_auctions_details,pg_insert_otomoto_auctions_stats
from scrapping.scrapping import get_otomoto_raw_cars_auctions
import logging
import datetime

logger = logging.getLogger(__name__)

def normalize(text):
    return re.sub(r'\s+', ' ', text.lower())

def auction_made_year_enriched(auction_title: str) -> int | None:
    """
    Docstring for auction_year_enriched
    
    :param auction_title: string
    :return: string
    """
    auction_title = normalize(auction_title)
    MADE_YEAR_PATTERN = re.compile(
    r"""
    (?:
        (?:rok|Rok|rb\.?|rok prod\.?|rok produkcji|prod\.?)  # słowa kluczowe
        [^\d]{0,30}
        (?<!\d)(19\d{2}|20[0-2]\d)(?!\d)
        |
        \b(?<!\d)(19\d{2}|20[0-2]\d)(?!\d)\s*r\.?\b
    )
    """,
    re.VERBOSE | re.IGNORECASE
    )
    
    match = MADE_YEAR_PATTERN.search(auction_title)
    if not match:
        return None
    year = match.group(1) or match.group(2)
    return int(year)
    
def auction_vin_enriched(auction_title: str) -> str | None:
    auction_title = normalize(auction_title)
    VIN_PATTERN = re.compile(
    r"""
    (?:VIN|nr\s+VIN|nr\s+nadw\.?)   # kontekst
    [^\w]{0,10}                     # separator
    ([A-HJ-NPR-Z0-9]{17})            # VIN
    """,
    re.VERBOSE | re.IGNORECASE
    )
    match = VIN_PATTERN.search(auction_title)
    
    if not match:
        return None
    vin_number = str(match.group(1))
    return vin_number

def auction_plates_enriched(auction_title: str) -> str | None:
    auction_title = normalize(auction_title)
    PLATE_PATTERN = re.compile(
    r"""
    (?:nr\s*rej\.?|rejestracja)     # kontekst
    [^\w]{0,10}                     # separator
    ([A-Z0-9]{2,3}                  # region
     (?:\s?[A-Z0-9]{2,5}))          # reszta
    """,
    re.VERBOSE | re.IGNORECASE
    )
    match = PLATE_PATTERN.search(auction_title)
    if not match:
        return None
    plate = match.group(1)
    plate = plate.replace(' ', '')
    return plate

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
        auction_title = i[0]
        brand = i[6] #brand
        if brand is None:
            continue
        models = get_car_brand_models(brand)
        model = auction_brand_model_enriched(i[0],models)
        id_list = get_car_brand_model_ids(brand,model)
        vin_number = auction_vin_enriched(auction_title)
        plate_number = auction_plates_enriched(auction_title)
        made_year = auction_made_year_enriched(auction_title)
        if len(id_list) > 0:    
            brand_id = id_list[0][0]
            model_id = id_list[0][1]
        else:
            brand_id = None
            model_id = None
        output_list.append(i[:6] + [brand_id,model_id,vin_number,plate_number,made_year])
    logger.info("Prepared list of " + str(len(output_list)) + " auctions to be inserted into pg at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return output_list

def otomoto_auctions():
    saved_debt_auctions = get_debt_car_auctions_details()
    for i in saved_debt_auctions:
        otomoto_auctions = []
        auction_id = i[0]
        make = i[1]
        make_id = i[2]
        model = i[3]
        model_id = i[4]
        m_year = i[5]
        auctions_list = []
        #scrapping
        total_count, auctions_list =  get_otomoto_raw_cars_auctions(make, model, m_year)
        if total_count in [None,0]:
            continue
        #calculating mean price and mileage
        mean_price = 0
        mean_mileage = 0
        df = pd.DataFrame(auctions_list)
        mean_price = df['price'].mean().__round__(0)
        mean_mileage = df['mileage'].mean().__round__(0)
        otomoto_auctions=[auction_id,make_id,model_id,m_year,total_count,mean_price,mean_mileage]
        pg_insert_otomoto_auctions_stats(otomoto_auctions)
        
    logger.info("Finished processing otomoto auctions for all " + str(len(saved_debt_auctions)) + " debt_auctions")


