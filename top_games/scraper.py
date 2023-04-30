import requests
from bs4 import BeautifulSoup
import pandas as pd


games_count = 1400
games = []
flag = True
page_num = 0
url = 'https://www.metacritic.com/browse/games/genre/metascore/strategy'

while flag:
    metacritic_response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(metacritic_response.content, "html.parser")
    game_blocks = soup.find_all("td", class_="clamp-summary-wrap")

    for block in game_blocks:
        title = block.find("h3").text.strip()

        platform = block.find("span", class_="data").text.strip()
        if platform == 'iOS':
            platform = 'Mobile'
        elif platform in ['Xbox 360', 'PlayStation 3', 'Xbox One', 'PlayStation 4', 'PlayStation 5']:
            platform = 'Consoles'
        elif platform in ['Switch', 'PC']:
            pass
        else:
            continue

        user_score = block.find('div', class_='clamp-userscore')
        user_score = user_score.find("div", class_='metascore_w').text.strip()
        if user_score == 'tbd':
            user_score = None
        else:
            user_score = int(float(user_score) * 10)

        meta_score = int(block.find("div", class_='metascore_w').text.strip())

        release_date = block.find('div', class_='clamp-details')
        release_date = release_date.find_all('span')[2].text.strip()

        # добавление в БД
        games.append({"title": title, "platform": platform, "metascore": meta_score, 'userscore': user_score, 'release_date': release_date})

        if len(games) == games_count:
            flag = False
            break

    page_num += 1
    if page_num > int(soup.find_all('a', class_='page_num')[-1].text.strip()):
        break
    url += f'?page={page_num}'
    print(page_num+1, len(games))


# for i in games:
#     print(i)
# print(len(games))


