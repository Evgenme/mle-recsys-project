import requests
import pandas as pd

events_store_url = "http://127.0.0.1:8020"
recommendations_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"
#items = pd.read_parquet("items.par")

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

params = {"user_id": 626829, 'k': 10}
resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

recs_offline = resp_offline.json()["recs"]
recs_online = resp_online.json()["recs"]
recs_blended = resp_blended.json()["recs"]

def display_items(item_ids):

    item_columns_to_use = ["item_id", "author", "title", "genre_and_votes", "average_rating", "ratings_count"]
    
    items_selected = items.query("item_id in @item_ids")[item_columns_to_use]
    items_selected = items_selected.set_index("item_id").reindex(item_ids)
    items_selected = items_selected.reset_index()
    
    print(items_selected)

event_item_ids =  [44184116, 70619465, 1710810, 33273]    
print("Онлайн-события")
#display_items(event_item_ids)
print(event_item_ids)
print("Офлайн-рекомендации")
#display_items(recs_offline)
print(recs_offline)
print("Онлайн-рекомендации")
#display_items(recs_online)
print(recs_online)
print("Рекомендации")
#display_items(recs_blended)
print(recs_blended)