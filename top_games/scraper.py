import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from time import time

df_comments = pd.DataFrame(columns=['title', 'developer', 'genre', 'comments'])


async def get_game_info(session, link):
    global df_comments

    comment_count_response = await session.get('https://www.metacritic.com' + link,
                                               headers={'User-Agent': 'Mozilla/5.0'})
    comment_soup = BeautifulSoup(await comment_count_response.text(), 'html.parser')

    title = comment_soup.find('h1').text.strip()
    try:
        developer = comment_soup.find('a', class_='button').text.strip()
    except AttributeError:
        developer = None
        print(title, 'have no developer :_(')

    genres = comment_soup.find_all('li', class_='product_genre')[0].find_all('span', class_='data')
    genre = '|'.join(genre.text.strip() for genre in genres)

    comment_blocks = comment_soup.find_all('li', class_='score_count')
    total = 0
    for block in comment_blocks:
        for elem in block.find_all('span', class_='count'):
            total += int(elem.text.strip().replace(',', ''))

    if title in list(df_comments['title']):
        # исключение повторений в developer, genre, title и подсчет коментариев(total)
        if developer not in df_comments.loc[df_comments['title'] == title, 'developer'].iloc[0].split('|'):
            df_comments.loc[df_comments['title'] == title, 'developer'] += f'|{developer}'
        elif any(item in genre.split('|') for item in
                 df_comments.loc[df_comments['title'] == title, 'genre'].iloc[0].split('|')):
            df_comments.loc[df_comments['title'] == title, 'genre'] = '|'.join(list(
                set(genre.split('|') + df_comments.loc[df_comments['title'] == title, 'genre'].iloc[0].split('|'))))

        df_comments.loc[df_comments['title'] == title, 'comments'] += total

    else:
        new_row = pd.DataFrame([{'title': title,
                                 'developer': developer,
                                 'genre': genre,
                                 'comments': total}])
        df_comments = pd.concat([df_comments, new_row], ignore_index=True)


async def main():
    df_games = pd.DataFrame(columns=['title', 'platform', 'metascore', 'userscore', 'release_date'])
    games_title = []
    games_count = 150
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
                release_date = block.find('div', class_='clamp-details').find_all('span')[2].text.strip()
                game_link = block.find('a', class_='title')['href']

                tasks.append(asyncio.create_task(get_game_info(session, game_link)))
                df_games[['metascore', 'userscore']] = df_games[['metascore', 'userscore']].fillna(0)

                # добавление в БД
                if title in games_title:
                    # исключение повторений в title и platform
                    if platform not in df_games.loc[df_games['title'] == title, 'platform'].iloc[0].split('|'):
                        df_games.loc[df_games['title'] == title, 'platform'] += f'|{platform}'
                    df_games.loc[df_games['title'] == title, 'metascore'] = int(
                        (df_games.loc[df_games['title'] == title, 'metascore'].iloc[0] + meta_score) / 2)
                    df_games.loc[df_games['title'] == title, 'userscore'] = int(
                        (df_games.loc[df_games['title'] == title, 'userscore'].iloc[0] + user_score) / 2)
                    df_games.loc[df_games['title'] == title, 'release_date'] += f'|{release_date}'
                else:
                    games_title.append(title)
                    df_games = pd.concat([df_games, pd.DataFrame([{'title': title,
                                                                   'platform': platform,
                                                                   'metascore': meta_score,
                                                                   'userscore': user_score,
                                                                   'release_date': release_date}])], ignore_index=True)
                if len(df_games) == games_count:
                    flag = False
                    break

            await asyncio.gather(*tasks)

            print('Watched', page_num + 1, 'page, taked', len(df_games), 'games.')
            page_num += 1
            if page_num > int(soup.find_all('a', class_='page_num')[-1].text.strip()):
                break
            url = f'https://www.metacritic.com/browse/games/genre/metascore/strategy?page={page_num}'

    game = df_games.merge(df_comments, how='inner', on='title')
    game.to_csv('top150_strategy.csv', index=False)


start_time = time()
asyncio.run(main())
print('Ended with ', time() - start_time, 'seconds.')
