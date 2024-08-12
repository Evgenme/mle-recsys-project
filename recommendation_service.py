import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
import pandas as pd
import requests

# Настройка логирования
logger = logging.getLogger("uvicorn.error")

features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """
        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info("Loaded")

    def get(self, user_id: int, k: int = 100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception as e:
            logger.error(f"No recommendations found: {e}")
            recs = []

        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]
    return ids

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")

# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн-рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    # получаем список последних событий пользователя, возьмём три последних
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    if resp.status_code != 200:
        logger.error(f"Failed to fetch events for user {user_id}: {resp.status_code}")
        return {"recs": []}
    
    events = resp.json()
    events = events.get("events", [])

    if not events:
        return {"recs": []}

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for track_id in events:
        # для каждого track_id получаем список похожих в item_similar_items
        params = {"track_id": track_id, "k": k}
        resp = requests.post(features_store_url + "/similar_items", headers=headers, params=params)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch similar items for item {track_id}: {resp.status_code}")
            continue
        
        item_similar_items = resp.json()
        items += item_similar_items.get("track_id_2", [])
        scores += item_similar_items.get("score", [])

    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    return {"recs": recs[:k]}

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs_offline = (await recommendations_offline(user_id, k))["recs"]
    recs_online = (await recommendations_online(user_id, k))["recs"]

    recs_blended = []
    min_length = min(len(recs_offline), len(recs_online))

    # Чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])  # Нечётные места (индексы 0, 2, 4, ...)
        recs_blended.append(recs_offline[i])  # Чётные места (индексы 1, 3, 5, ...)

    # Добавляем оставшиеся элементы в конец
    recs_blended.extend(recs_online[min_length:])
    recs_blended.extend(recs_offline[min_length:])

    # Удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # Оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}

rec_store = Recommendations()

# Загрузка рекомендаций
rec_store.load(
    "personal",
    "recommendations.parquet",
    columns=["user_id", "track_id", "score"],
)
rec_store.load(
    "default",
    "top_recs.parquet",
    columns=["track_id", "rank"],
)
