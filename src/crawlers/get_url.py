# Lấy url để đào data cho từng film
import time
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import json
def load_existing_data(file_path):
    """Load existing data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return [] 
    except json.JSONDecodeError:
        return []  
    
# def get_raw_url(page):
#     urls_data = load_existing_data('url_raw.json')
#     table = page.find("table", {"class": "a-bordered a-horizontal-stripes a-size-base a-span12 mojo-body-table mojo-table-annotated mojo-body-table-compact scrolling-data-table"})
#     i = 0
#     for tr in table.find_all("tr"):
#         td = tr.find("td", {"class": "a-text-left mojo-field-type-release mojo-cell-wide"})
#         # print(i)
#         i+=1
#         if td:
#             a_tag = td.find("a")
#             if a_tag and 'href' in a_tag.attrs:
#                 href = a_tag['href']
#                 urls_data.append(('https://www.boxofficemojo.com' + href,False))
#     with open("url_raw.json", "w", encoding="utf-8") as json_file:
#         json.dump(urls_data, json_file, ensure_ascii=False, indent=4)

def transform_url(driver,url):
    driver.get(url)
    page = BeautifulSoup(driver.page_source, features="html.parser")
    table = page.find("a", {"class": "a-link-normal mojo-title-link refiner-display-highlight"})
    new_url = 'https://www.boxofficemojo.com' + table['href']
    time.sleep(5)
    driver.get(new_url)
    page = BeautifulSoup(driver.page_source, features="html.parser")
    table = page.find("a", {"class": "a-size-base a-link-normal mojo-navigation-tab"})
    new_url = 'https://www.boxofficemojo.com' + table['href']
    time.sleep(5)
    urls_data = load_existing_data('true_url.json')
    if new_url not in urls_data:
        urls_data.append(new_url)
        with open("true_url.json", "w", encoding="utf-8") as json_file:
            json.dump(urls_data, json_file, ensure_ascii=False, indent=4)
        return new_url
    else:
        return False

