# Đây là đào data về ranking --> ranking.json
from bs4 import BeautifulSoup
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from get_url import  transform_url
from get_film_info import CrawlInfo

def load_existing_data(file_path):
    """Load existing data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return [] 
    except json.JSONDecodeError:
        return []  
    


url = 'https://www.boxofficemojo.com/date/{date}/?ref_=bo_da_nav'

def create_data():
    data = {
        'Date': None,
        'Day': None,
        'Month': None,
        'Year': None,
        'Movies': []
    }
    return data

idx_to_key = ["Rank", "Rank Yesterday", "Release", "Daily Gross","Gross Change/Day" ,"%± LW", "Theaters", "Average", "Gross to Date", "Days", "Distributor", "New"]


def get_all_dates(start_day,start_month,end_day,end_month,start_year,end_year):
    # Bắt đầu từ ngày 1 tháng 1 của năm
    start_date = datetime(start_year, start_month, start_day)
    # Kết thúc vào ngày 31 tháng 12 của năm
    end_date = datetime(end_year, end_month, end_day)
    
    # Danh sách để lưu trữ các ngày
    all_dates = []
    
    # Sử dụng vòng lặp để lấy tất cả các ngày trong năm
    current_date = start_date
    while current_date <= end_date:
        # Thêm ngày vào danh sách với định dạng YYYY-MM-DD
        all_dates.append(current_date.strftime('%Y-%m-%d'))
        # Tăng ngày lên 1
        current_date += timedelta(days=1)
    
    return all_dates
dates = get_all_dates(
    start_day=10,
    start_month=6,
    end_day=1,
    end_month=7,
    start_year=2024,
    end_year=2024
)

dates.reverse()

# dates = ['2024-11-07', '2024-11-06', '2024-11-05', '2024-11-04', '2024-11-03', '2024-11-02', '2024-11-01']

chrome_options = Options()
chrome_options.add_argument("--incognito")
driver = webdriver.Chrome(options=chrome_options)

for date in tqdm(dates, desc="Processing dates"):
    # Thực hiện các hành động với từng ngày
    print(f"Processing date: {date}")
    url = 'https://www.boxofficemojo.com/date/{}/?ref_=bo_da_nav'.format(date)
    
    Bigdata = load_existing_data('Bigdata.json')
    data = create_data()
    
    data['Date'] = date
    data['Day'] = date.split('-')[2]
    data['Month'] = date.split('-')[1]
    data['Year'] = date.split('-')[0]
    driver.get(url)

    list_urls = []
    page = BeautifulSoup(driver.page_source, features="html.parser")
    
    table = page.find("table", {"class": "a-bordered a-horizontal-stripes a-size-base a-span12 mojo-body-table mojo-table-annotated mojo-body-table-compact scrolling-data-table"})
    for id, tr in enumerate(table.find_all("tr")):
        if id == 0:  # Skip header row
            continue
        movie_sample = {key: None for key in idx_to_key}
        length = len(tr.find_all("td"))
        for idx, td in enumerate(tr.find_all("td")):
            if idx == length - 1:
                break
            else:
                movie_sample[idx_to_key[idx]] = td.get_text().replace('\n', '').strip()
                if (idx == 2):
                    title = movie_sample[idx_to_key[idx]]
                    a_tag = td.find("a")
                    if a_tag and a_tag.has_attr("href"):
                        href = a_tag["href"]     
                        list_urls.append(['https://www.boxofficemojo.com' + href,title])              
        data['Movies'].append(movie_sample)
    Bigdata.append(data)
    with open("Bigdata.json", "w", encoding="utf-8") as json_file:
        json.dump(Bigdata, json_file, ensure_ascii=False, indent=4)
    for url in list_urls:
        new_url = transform_url(driver,url[0])
        if(new_url):
            crawler  = CrawlInfo(driver)
            crawler.save_film(new_url,url[1])
            time.sleep(5)
    
    time.sleep(5)
driver.close()


