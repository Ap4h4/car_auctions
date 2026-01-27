from playwright.sync_api import sync_playwright
import re
import unicodedata
import pandas as pd 

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
        print(len(models), models)

    return models
   
def get_brands_models():
    #output model
    df = pd.DataFrame(columns=['brand', 'model'])
    #scrapping and cleaning brands
    brands = scrapping_brands()
    brands = cleaning_brands(brands)

    #scrapping models for each of the brands
    for brand in brands:
        models = scrapping_models(brand)
        models = cleaning_models(models)
        df = pd.concat([df, pd.DataFrame({'brand': [brand] * len(models), 'model': models})], ignore_index=True)

    #saving to csv
    df.to_csv('brands_models.csv', index=False)

    return df





if __name__ == "__main__":
    brands = get_brands()
    brands = cleaning_brands(brands)

        #print(len(brands), brands)

    