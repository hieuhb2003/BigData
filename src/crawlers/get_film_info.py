# import pandas as pd
# from bs4 import BeautifulSoup
# from loguru import logger
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
# import json
# def process_text(text):
#     text = text.replace('\n', '').strip()
#     text = ' '.join(text.split())
#     phrase_to_remove = "See full company information"
#     if phrase_to_remove in text:
#         text = text.replace(phrase_to_remove, '').strip()
#     if '(' in text and ')' in text:
#         text = text.split('(')
#         return [text[0].strip(), text[1].replace(')', '').strip()] # chữ 1 là date, chữ 2 là country
#     else :
#         return [text]
# film = {
#     'Title': None,
#     'Earliest Release Country': None,
#     'Domestic Distributor': None,
#     'Domestic Opening': None,
#     'Earliest Release Date': None,
#     'MPAA': None,
#     'Running Time': None,
#     'Genres': None,
#     'Director': None,
#     'Writer': None,
#     'Producer': None,
#     'Composer': None,
#     'Cinematographer': None,
#     'Editor': None,
#     'Production Designer': None,
#     'Actors': None,
#     'Url': None
# }

# # Đọc nội dung từ file HTML đã lưu
# with open("page2.html", "r", encoding="utf-8") as file:
#     page_source = file.read()

# # Phân tích cú pháp HTML
# page = BeautifulSoup(page_source, features="html.parser")
# table = page.find("div", {"class": "a-section a-spacing-none mojo-summary-values mojo-hidden-from-mobile"})
# entity = table.find_all("div", {"class": "a-section a-spacing-none"})
# for x in entity:
#     spans = x.find_all("span")
#     if len(spans) > 1:
#         key = spans[0].get_text().replace('\n', '').strip()
#         if key in film:
#             span_text_lst = process_text(spans[1].get_text())
#             if(len(span_text_lst) == 1):
#                 value = span_text_lst[0]
#                 film[key] = value
#             else:
#                 film[key] = span_text_lst[0]
#                 film['Earliest Release Country'] = span_text_lst[1]

# table = page.find("table", {"id": "principalCrew"})
# tr = table.find_all("tr")
# for i in range(1, len(tr)):
#     td = tr[i].find_all("td")
#     value = td[0].get_text().replace('\n', '').strip()
#     key = td[1].get_text().replace('\n', '').strip()
#     if key in film:
#         if film[key] is None:
#             film[key] = value
#         else:
#             if len(film[key]) > 1 and type(film[key]) == list:
#                 film[key].append(value)
#             else:
#                 film[key] = [film[key]]
#                 film[key].append(value)



# table = page.find("table", {"id": "principalCast"})
# tr = table.find_all("tr")
# for i in range(1, len(tr)):
#     td = tr[i].find_all("td")
#     value = td[0].get_text().replace('\n', '').strip()
#     if film['Actors'] is None:
#         film['Actors'] = [value]
#     else:  
#         film['Actors'].append(value)


# with open("film.json", "w", encoding="utf-8") as json_file:
#     json.dump(film, json_file, ensure_ascii=False, indent=4)

# def get_film_info(url):
#     driver = webdriver.Chrome()
#     driver.get(url)
#     page = BeautifulSoup(driver.page_source, features="html.parser")
#     table = page.find("a", {"class": "a-link-normal mojo-title-link refiner-display-highlight"})
#     new_url = 'https://www.boxofficemojo.com' + table['href']
#     driver.get(new_url)
#     page = BeautifulSoup(driver.page_source, features="html.parser")
#     table = page.find("a", {"class": "a-size-base a-link-normal mojo-navigation-tab"})
#     new_url = 'https://www.boxofficemojo.com' + table['href']
#     driver.get(new_url)
#     page = BeautifulSoup(driver.page_source, features="html.parser")
#     with open("page2.html", "w", encoding="utf-8") as file:
#         file.write(str(page))
#     driver.close()
#     return

from bs4 import BeautifulSoup
import json

class CrawlInfo():
    def __init__(self, driver):
        self.driver = driver
        
    def create_film(self):
        film = {
            'Title': None,
            'Earliest Release Country': None,
            'Domestic Distributor': None,
            'Domestic Opening': None,
            'Earliest Release Date': None,
            'MPAA': None,
            'Running Time': None,
            'Genres': None,
            'Director': None,
            'Writer': None,
            'Producer': None,
            'Composer': None,
            'Cinematographer': None,
            'Editor': None,
            'Production Designer': None,
            'Actors': None,
        }
        return film     
    def process_text(self, text):
        text = text.replace('\n', '').strip()
        text = ' '.join(text.split())
        phrase_to_remove = "See full company information"
        if phrase_to_remove in text:
            text = text.replace(phrase_to_remove, '').strip()
        if '(' in text and ')' in text:
            text = text.split('(')
            return [text[0].strip(), text[1].replace(')', '').strip()]  # phần đầu là date, phần sau là country
        else:
            return [text]

    def get_film_info(self,url,title):
        film =self.create_film()
        film['Title'] = title
        self.driver.get(url)
        page = BeautifulSoup(self.driver.page_source, features="html.parser")
        
        # Lấy thông tin từ bảng summary
        table = page.find("div", {"class": "a-section a-spacing-none mojo-summary-values mojo-hidden-from-mobile"})
        if table:
            entity = table.find_all("div", {"class": "a-section a-spacing-none"})
            for x in entity:
                spans = x.find_all("span")
                if len(spans) > 1:
                    key = spans[0].get_text().replace('\n', '').strip()
                    if key in film:
                        span_text_lst = self.process_text(spans[1].get_text())
                        if len(span_text_lst) == 1:
                            value = span_text_lst[0]
                            film[key] = value
                        else:
                            film[key] = span_text_lst[0]
                            film['Earliest Release Country'] = span_text_lst[1]

        # Lấy thông tin từ bảng principalCrew
        table = page.find("table", {"id": "principalCrew"})
        if table:
            tr = table.find_all("tr")
            for i in range(1, len(tr)):
                td = tr[i].find_all("td")
                if len(td) > 1:
                    value = td[0].get_text().replace('\n', '').strip()
                    key = td[1].get_text().replace('\n', '').strip()
                    if key in film:
                        if film[key] is None:
                            film[key] = value
                        else:
                            if isinstance(film[key], list):
                                film[key].append(value)
                            else:
                                film[key] = [film[key], value]

        # Lấy thông tin từ bảng principalCast
        table = page.find("table", {"id": "principalCast"})
        if table:
            tr = table.find_all("tr")
            for i in range(1, len(tr)):
                td = tr[i].find_all("td")
                if td:
                    value = td[0].get_text().replace('\n', '').strip()
                    if film['Actors'] is None:
                        film['Actors'] = [value]
                    else:
                        film['Actors'].append(value)
        return film
    
    def save_film(self, url, title, path='film.json'):
        # Lấy thông tin phim từ URL
        film = self.get_film_info(url,title)
        
        # Mở file JSON để thêm dữ liệu mới vào
        try:
            with open(path, "r", encoding="utf-8") as file:
                data = json.load(file)
        except FileNotFoundError:
            # Nếu file chưa tồn tại, khởi tạo danh sách rỗng
            data = []
        
        # Thêm thông tin phim vào danh sách
        if film not in data:
            data.append(film)
        
        # Lưu lại vào file JSON
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
