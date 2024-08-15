import requests
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(filename='test_service.log', 
                    filemode='w', 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

events_store_url = "http://127.0.0.1:8020"
recommendations_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"

items = pd.read_parquet("items_frac.parquet")
catalog = pd.read_parquet("cat.parquet")

# Параметры запроса к микросервису
new_user_id = 555329
user_id = 626829
k = 5
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": int(user_id), 'k': int(k)}
event_item_ids = [91991232, 86743902, 86743902, 28038563]

# Отправляем треки из списка в events_service
for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                         headers=headers, 
                         params={"user_id": int(user_id), "track_id": int(event_item_id)})
    if resp.status_code == 200:
        logger.info("Track added successfully.")
    else:
        logger.error("Failed to add track.")

resp_top = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params={"user_id": int(new_user_id), 'k': int(k)})
resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

resp_top = resp_top.json()["recs"]
recs_offline = resp_offline.json()["recs"]
recs_online = resp_online.json()["recs"]
recs_blended = resp_blended.json()["recs"]

# Функция для добавления имен по id
def add_name(cell_val, col):
    if isinstance(cell_val, int):
        return catalog.loc[(catalog['type'] == col) & (catalog['id'] == cell_val), 'name'].values[0] if not catalog[(catalog['type'] == col) & (catalog['id'] == cell_val)].empty else None
    elif isinstance(cell_val, object):
        return [catalog.loc[(catalog['type'] == col) & (catalog['id'] == x), 'name'].values[0] for x in cell_val if not catalog[(catalog['type'] == col) & (catalog['id'] == x)].empty]
    else:
        return None

def display_items(list, title):
    df = pd.DataFrame(list, columns=['track_id'])
    df = df.merge(items, on='track_id', how='left')
    df['genre'] = df['genres'].apply(lambda x: add_name(x, 'genre'))
    df['artist'] = df['artists'].apply(lambda x: add_name(x, 'artist'))
    df['album'] = df['albums'].apply(lambda x: add_name(x, 'album'))
    df['title'] = df['track_id'].apply(lambda x: add_name(x, 'track'))
    
    logger.info(title)
    logger.info(df[['track_id','title','artist','album','genre']].to_string(index=False))

logger.info("Рекомендации для пользователя без персональных рекомендаций")
display_items(resp_top, "Офлайн-рекомендации")

logger.info("Офлайн-рекомендации для пользователя с персональными рекомендациями, но без онлайн-истории")
display_items(recs_offline, "Офлайн-рекомендации")

logger.info("Онлайн-события")
display_items(event_item_ids, "События")

logger.info("Онлайн-рекомендации")
display_items(recs_online, "Онлайн-рекомендации")

logger.info("Рекомендации для пользователя с персональными рекомендациями и онлайн-историей.")
display_items(recs_blended, "Смешанные рекомендации")
