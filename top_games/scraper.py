import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from time import time


async def get_comments_count(session, link, comments_list):
    comment_count_response = await session.get('https://www.metacritic.com' + link,
                                               headers={'User-Agent': 'Mozilla/5.0'})
    comment_soup = BeautifulSoup(await comment_count_response.text(), 'html.parser')
    comment_blocks = comment_soup.find_all('li', class_='score_count')
    title = comment_soup.find('h1').text.strip()

    total = 0
    for block in comment_blocks:
        for elem in block.find_all('span', class_='count'):
            total += int(elem.text.strip().replace(',', ''))

    comments_list.append({'title': title,
                          'comments': total})


async def main():
    games_count = 150
    games = []
    comments = []
    flag = True
    page_num = 0
    url = 'https://www.metacritic.com/browse/games/genre/metascore/strategy'

    async with aiohttp.ClientSession() as session:
        while flag:
            metacritic_response = await session.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(await metacritic_response.text(), 'html.parser')
            game_blocks = soup.find_all('td', class_='clamp-summary-wrap')

            tasks = []
            for block in game_blocks:
                title = block.find('h3').text.strip()

                platform = block.find('span', class_='data').text.strip()
                if platform == 'iOS':
                    platform = 'Mobile'
                elif platform in ['Xbox 360', 'PlayStation 3', 'Xbox One', 'PlayStation 4', 'PlayStation 5']:
                    platform = 'Consoles'
                elif platform in ['Switch', 'PC']:
                    pass
                else:
                    continue

                user_score = block.find('div', class_='clamp-userscore')
                user_score = user_score.find('div', class_='metascore_w').text.strip()
                if user_score == 'tbd':
                    user_score = None
                else:
                    user_score = int(float(user_score) * 10)

                meta_score = int(block.find("div", class_='metascore_w').text.strip())

                release_date = block.find('div', class_='clamp-details')
                release_date = release_date.find_all('span')[2].text.strip()

                comments_link = block.find('a', class_='title')['href']
                tasks.append(asyncio.create_task(get_comments_count(session, comments_link, comments)))

                # добавление в БД
                games.append({'title': title,
                              'platform': platform,
                              'metascore': meta_score,
                              'userscore': user_score,
                              'release_date': release_date})
                if len(games) == games_count:
                    flag = False
                    break

            await asyncio.gather(*tasks)

            print('Watched', page_num + 1, 'page, taked', len(games), 'games.')
            page_num += 1
            if page_num > int(soup.find_all('a', class_='page_num')[-1].text.strip()):
                break
            url = f'https://www.metacritic.com/browse/games/genre/metascore/strategy?page={page_num}'

    df_games = pd.DataFrame(games)
    df_comments = pd.DataFrame(comments)
    df_games.to_csv('games.csv', index=False)
    df_comments.to_csv('comments.csv', index=False)

start_time = time()
asyncio.run(main())
print('Ended with ', time() - start_time, 'seconds.')
