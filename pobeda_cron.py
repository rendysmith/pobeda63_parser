from bs4 import BeautifulSoup
import pandas as pd
#from lxml import html
import requests
from tqdm import tqdm
import time
import threading
import sqlite3
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

db_name = 'pobeda.db'

def send_telegram(text: str):
    token = config['Telegram']['token']
    url = "https://api.telegram.org/bot"
    channel_id = config['Telegram']['channel_id']
    url += token
    method = url + "/sendMessage"

    r = requests.post(method, data={
        "chat_id": channel_id,
        "text": text
          })

    if r.status_code != 200:
        raise Exception("post_text error")

def create_table():
    conn = sqlite3.connect(db_name)  # создание БД если нет. Если есть то подключение.
    cur = conn.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}(
    Unix_time INT,
    Name TEXT,
    Price REAL,
    Link TEXT);
    """)
    conn.commit()
    conn.close()

def into_new_date(data_list):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    name = data_list[0]
    price = data_list[1]
    link = data_list[2]
    city = data_list[3]
    cur.execute(f"SELECT Link FROM {table_name} WHERE Link=?;", [link])

    if cur.fetchone() is None:
        txt = f'Name: {name}\nPrice: {price:,.0f} р.\nГород: {city}\n\n{link}'
        black_list = ['band 6']
        try:
            if all(black not in name.lower() for black in black_list):
                send_telegram(txt)
            cur.execute(f"INSERT INTO {table_name} VALUES(?, ?, ?, ?);", [int(time.time()), name, price, link])
            conn.commit()
            print(f'Commit! {link}')
            time.sleep(3)
        except Exception as EX:
            send_telegram(EX)
            print('Ошибка отправки сообщения!')
            time.sleep(5)

    else:
        print(f'Уже есть в базе - {link}')
    conn.close()

def get_url_data(page):
    #url = f'https://победа-63.рф/catalog/telefony/umnye-chasy-i-braslety/{page}/?q=60&s=low&c=0&cg=578&a=0&min=1000&max=5000&'
    #url = f'https://победа-63.рф/catalog/telefony/naushniki-dlya-telefonov/{page}/?k=&q=60&s=low&c=0&cg=0&a=0&page=2&min=500&max=5000&'
    url = 'https://победа-63.рф/catalog/telefony/umnye-chasy-i-braslety/?k=&q=60&s=new&c=0&cg=0&a=0&min=1000&max=5000&'
    global table_name
    table_name = url.split('/')[5].replace('-', '_')
    r = requests.get(url).text
    return BeautifulSoup(r, "html.parser")

def main():
    #url = 'https://победа-63.рф/catalog/telefony/umnye-chasy-i-braslety/?q=60&s=low&c=0&cg=578&a=0&min=1000&max=120000&'
    soup = get_url_data(1)
    create_table()
    pages = int(soup.find('span', class_='filter-pagination--number').text)
    pages = 1

    rows = []
    for page in range(pages):
        soup = get_url_data(page + 1)
        blocks = soup.find_all('div', class_='card is-lazy')
        print(f"{page+1} из {pages}", len(blocks))

        for block in blocks:
            name = block.find('a', class_='card-title').text.strip()
            reserv = block.find('div', {'class': 'card-labels'}).text
            if reserv != 'Забронировано':
                price = block.find('div', {'class': 'card-price', 'itemprop': 'price'}).get('content')
                city = block.find('div', class_='card-city').text
                price = float(price)
                link = 'https:' + block.find('a', class_='card-title').get('href')
                data_list = [name, price, link, city]
                #rows.append(data_list)
                into_new_date(data_list)

            else:
                print(f'{name} - Забронировано')

        time.sleep(1)
        #input()

    #df = pd.DataFrame(rows, columns=['Name', 'Price', 'Link'])
    #df.to_excel(f'{table_name}.xlsx', index=False)

main()
