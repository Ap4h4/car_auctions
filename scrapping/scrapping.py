import requests
from bs4 import BeautifulSoup
import re
import sys
import os
import pandas as pd 
import json
import datetime
import logging
from playwright.sync_api import sync_playwright
import unicodedata
from db_connectors.postgressql_connector import connect_to_db as pg_connect_to_db, pg_insert_car_makes, pg_insert_car_models


logger = logging.getLogger(__name__)


"""
    Function for scrapping data from https://licytacje.komornik.pl
"""
def debt_auctions_scrapper():
    
    #getting full site
    url = 'https://licytacje.komornik.pl' #24 to numer kategorii
    cars_url = '/Notice/Filter/24'
    page_number = 1
    logger.info("Starting scrapping at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #objects for saving data
    df = pd.DataFrame(columns=['auction_date', 'auction_city', 'auction_region','auction_item', 'starting_price','target_price','auction_link','item_ts'])
    date_pattern = r'\d{2}.\d{2}.\d{4}'

    #json for saving data
    data = {
        "item_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "details": []
    }
    #iterating through site pages and populating url list
    while True:
        respones = requests.get(url+cars_url+'?page='+str(page_number))
        logger.info("Scraping url "+url+cars_url+'?page='+str(page_number))
        soup = BeautifulSoup(respones.content, 'html.parser')   
        table = soup.find('table', class_='wMax')
        table_rows = table.find_all('tr')

        for i,tr in enumerate(table_rows):
            if i != 0:
                tr_cells = tr.find_all('td')
                #extracting sublink to the car details
                link_td = tr_cells[-1]
                link = url + link_td.find('a').get('href')


                #extracting date of auction
                date_match = re.search(date_pattern,tr_cells[2].text)
                if date_match:
                    auction_date = date_match.group()

                #extracting location of auction
                pre_location = tr_cells[4].text.strip().split('\n')
                auction_city = pre_location[0].strip()
                pre_auction_region = pre_location[1].strip()
                auction_region = pre_auction_region.replace('(', '').replace(')', '')

                #extracting starting price - overwritten by sub_table details if exists
                pre_starting_price = tr_cells[5].text.strip()
                pre_starting_price = pre_starting_price.split(',')[0]
                starting_price = float(pre_starting_price.replace('zł', '').replace(' ', '').replace(',','.').replace('\xa0',''))

                #getting auction details
                sub_request = requests.get(link)
                sub_soup = BeautifulSoup(sub_request.content, 'html.parser')
                starting_price_ind = 3
                target_price_ind = 2

                sub_table = sub_soup.find('table')
                if sub_table:
                    sub_table_rows = sub_table.find_all('tr')
                    sub_table_headers = sub_table.find_all('th')

                    for i1,th in enumerate(sub_table_headers):
                        if th.text.strip() == 'Cena wywołania':
                            starting_price_ind = i1
                        elif th.text.strip() == 'Suma oszacowania':
                            target_price_ind = i1

                    for i2,tr2 in enumerate(sub_table_rows):
                        if i2 != 0:
                            sub_tr_cells = tr2.find_all('td')
                            auction_item = sub_tr_cells[1].text.strip()
                            pre_target_price = sub_tr_cells[target_price_ind].text.strip()
                            pre_target_price = pre_target_price.split(',')[0]
                            target_price = float(pre_target_price.replace('zł', '').replace(' ', '').replace(',','.').replace('\xa0',''))

                            pre_starting_price = sub_tr_cells[starting_price_ind].text.strip()
                            pre_starting_price = pre_starting_price.split(',')[0]
                            starting_price = float(pre_starting_price.replace('zł', '').replace(' ', '').replace(',','.').replace('\xa0',''))
                            item_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            #print(auction_date, auction_city,auction_region,auction_item,starting_price,target_price,item_ts)
                            #saving to dataframe
                            df.loc[len(df)] = [auction_date, auction_city,auction_region,auction_item,starting_price,target_price,link,item_ts]



        #checking if exists next page link on the current site
        page_number += 1
        #page_number = 12
        next_url = '/Notice/Filter/24?page='+ str(page_number)
        next_page = soup.find('a', href = lambda href: (href and href.startswith(next_url)))
        if next_page is None:
            logger.info("Finished scrapping at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            break
    return df

"""
Scrapping functions for OtoMoto car models dictionary
"""

def normalizing_brand_name(brand_name):
    #removing numbers and special characters
    normalized_brand_name = re.sub(r'\s*\(\d+\)$', '', brand_name)

    #lowering
    normalized_brand_name = normalized_brand_name.lower()
    
    #removing polish diacritics
    normalized_brand_name = (
        unicodedata
        .normalize("NFKD", normalized_brand_name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

    #replacing spaces with hyphens
    normalized_brand_name = re.sub(r'\s+', '-', normalized_brand_name)

    return normalized_brand_name

def cleaning_brands(brands):
    brands_cleaned = list(map(lambda x: normalizing_brand_name(x), brands))
    return brands_cleaned

def scrapping_brands():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto("https://www.otomoto.pl/osobowe", timeout=60_000)

        # accepting cookies
        page.get_by_role("button", name="Akceptuj").click()

        # hitting input
        page.get_by_role("textbox", name="Marka pojazdu").click()

        page.wait_for_selector("[role='option']")
        #collecting all brands
        brands = page.locator("[role='option']").all_inner_texts()
        #removing first element which is "wszystkie"
        brands = brands[1:]
        browser.close()
    return brands
        

def normalizing_model_name(model_name):
    #lowering
    normalized_model_name = model_name.lower()
    #removoing polish diacritics
    normalized_model_name = (
            unicodedata
            .normalize("NFKD", normalized_model_name)
            .encode("ascii", "ignore")        
            .decode("ascii")
    )
    return normalized_model_name

def cleaning_models(models):
    #removing counted models
    models_cleaned = [item.split(' (')[0] for item in models if int(item.split(' (')[1][:-1]) > 0] 
    models_cleaned = list(map(lambda x: normalizing_model_name(x), models_cleaned))
    return models_cleaned

def scrapping_models(brand):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto("https://www.otomoto.pl/osobowe/"+brand+"", timeout=60_000)

        # cookies – MUSI być
        page.get_by_role("button", name="Akceptuj").click()

        # klikamy INPUT, nie combobox
        page.get_by_role("textbox", name="Model pojazdu").click()

        page.wait_for_selector("[role='option']")

        models = page.locator("[role='option']").all_inner_texts()
        #removing first element which is "wszystkie"
        models = models[1:]
        browser.close()
        

    return models
   
def get_raw_cars_brands():
    logger.info("Starting scrapping car brands from OtoMoto at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    brands = scrapping_brands()
    logger.info("Scrapped car brands " + str(len(brands)) + " from OtoMoto at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    brands = cleaning_brands(brands)
    return brands

def get_raw_cars_brand_models(brand):
    try:
        #output model dataframe
        df = pd.DataFrame(columns=['brand', 'model'])
        logger.info("Starting scrapping car models for brand " + brand + " from OtoMoto at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        models = scrapping_models(brand)
        models = cleaning_models(models)
        logger.info("Scrapped " + str(len(models)) + " car models for " + brand + " from OtoMoto at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        df = pd.concat([df, pd.DataFrame({'brand': [brand] * len(models), 'model': models})], ignore_index=True)
        logger.info("Scrapped all car models from OtoMoto at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        #output dictionary
        dict_brand_models = df.groupby('brand')['model'].apply(list).to_dict()
        return dict_brand_models
    except Exception as e:
        logger.exception(
            f"Error while scraping car models for brand {brand}: {e}"
        )
        #returing empty dict to not intercept ETL pipeline
        return {}
    
"""
Scrapping functions for OtoMoto same auctions as debt auctions
"""
def get_otomoto_raw_cars_auctions(brand, model, year):
    """
    Docstring for get_raw_cars_auctions:
    Function gets raw data from OtoMoto for given brand, model and madeyear+-2 (optional)
    
    :param brand: Description
    :param model: Description
    :param year: Description
    :return: total_count of all ads and list of tuples for each add: price, mileage, year

    """
    if year == None:
        url = "https://www.otomoto.pl/osobowe/"+brand+"/"+model+""
    else:
        start_year = int(year) - 2
        end_year = int(year) + 2
        start_year = str(start_year)
        end_year = str(end_year)
        url = "https://www.otomoto.pl/osobowe/"+brand+"/"+model+"/od-"+str(year)+"?search%5Bfilter_float_year%3Ato%5D="+end_year+""
    
    with sync_playwright() as p:
        
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        if not page:
            logger.error("Missing page - otomoto not rendered")
        # cookies – MUSI być
        page.get_by_role("button", name="Akceptuj").click()
        
        html = page.content()
        if not html:
            logger.error("Missing HTML - otomoto not rendered")
        page.wait_for_timeout(3000)
        browser.close()

        soup = BeautifulSoup(html, "lxml")
        #Total count of ads
        total_count_p= soup.find("p", class_="efp1nuf2")
        if not total_count_p:
            logger.error("Missing <p> tag for total count - total count not collected")
        else:
            total_count = total_count_p.find("b") if total_count_p else None
            total_count = (
                                int(re.sub(r"\D", "", total_count.get_text()))
                                if total_count else None
                            )
        
        
        # Main page articles
        main = soup.find("main")
        if not main:
            logger.error("Missing <main> - otomoto not rendered")  
        results_div = main.find("div", attrs={"data-testid": "search-results"})
        if not results_div:
            logger.error("No auctions on otomoto for brand and model: " + str(brand) + " " + str(model))
            return None, None
        else:
            articles = results_div.find_all("article", class_="e1srzcph1")
        results = []

        for a in articles:
            # ---- PRICE ----
            #text = a.get_text(" ", strip=True).lower()
            if not a.find("h3", class_="eg88ra81"):
                logger.error("Missing <h3> tag for price - price not collected")
                continue
            raw_price = a.find("h3", class_="eg88ra81").get_text(" ", strip=True).lower()
            price = int(re.sub(r"[^\d]", "", raw_price))
            

            # ---- MADE YEAR ----
            if not a.find("dd", attrs={"data-parameter": "year"}):
                logger.error("Missing <dd> tag for year - year not collected")
                continue
            raw_year = a.find("dd", attrs={"data-parameter": "year"}).get_text(" ", strip=True).lower()
            year = int(raw_year)

            # ---- MILAGE ----
            if not a.find("dd", attrs={"data-parameter": "mileage"}):
                logger.error("Missing <dd> tag for mileage - mileage not collected")
                continue    
            raw_mileage = a.find("dd", attrs={"data-parameter": "mileage"}).get_text(" ", strip=True).lower()
            mileage = int(re.sub(r"[^\d]", "", raw_mileage))

            results.append({
                "price": price,
                "year": year,
                "mileage": mileage
            })

        return total_count,results





  


