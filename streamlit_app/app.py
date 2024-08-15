import streamlit as st
import requests
import pandas as pd
import os
import random

# URL микросервиса и данных
recommendations_url = "http://recommendation_service:8000"
events_url = "http://events_service:8020"
data_path = "/app/data"

st.title("Music Recommendation Service")

# Функция для получения случайного user_id из файла
@st.cache
def get_random_user_id():
    df = pd.read_parquet(data_path + '/recommendations.parquet')
    random_user_id = random.choice(df['user_id'].tolist())
    return random_user_id

# Поля ввода для User ID и количества рекомендаций
user_id = st.sidebar.number_input("Enter User ID:", min_value=0, value=get_random_user_id())
k = st.sidebar.number_input("Number of Recommendations:", min_value=1, value=5)
input_track_id = st.sidebar.number_input("Enter Track ID:", min_value=0)

# Параметры запроса к микросервису
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": int(user_id), 'k': int(k)}

# Поле для отображения последних треков
recent_tracks = st.empty()
# Получение и отображение списка последних треков
response = requests.post(
    events_url + "/get",
    params=params
)
if response.status_code == 200:
    recent_tracks_list = response.json().get('events', [])
    recent_tracks.write(f"Recent Tracks: {recent_tracks_list}")
else:
    st.error("Failed to retrieve recent tracks.")
recent_tracks.write(f"Recent Tracks: {recent_tracks_list}")

# Функция для получения случайного track_id из файла
def get_random_track_id():
    df = pd.read_parquet(data_path + '/items_frac.parquet')
    random_track_id = random.choice(df['track_id'].tolist())
    return random_track_id

# Кнопка Add track
if st.sidebar.button("Add track"):
    st.write(f"Selected random track ID: {input_track_id}")

    # Отправляем случайный трек в events_service
    response = requests.post(
        events_url + "/put",
        params={"user_id": int(user_id), "track_id": int(input_track_id)}
    )

    if response.status_code == 200:
        st.success("Track added successfully.")
    else:
        st.error("Failed to add track.")

    # Получение и отображение списка последних треков
    response = requests.post(
        events_url + "/get",
        params={"user_id": int(user_id), "k": 5}
    )

# Кнопка Add random track
if st.sidebar.button("Add random track"):
    random_track_id = get_random_track_id()
    st.write(f"Selected random track ID: {random_track_id}")

    # Отправляем случайный трек в events_service
    response = requests.post(
        events_url + "/put",
        params={"user_id": int(user_id), "track_id": int(random_track_id)}
    )

    if response.status_code == 200:
        st.success("Track added successfully.")
    else:
        st.error("Failed to add track.")

    # Получение и отображение списка последних треков
    response = requests.post(
        events_url + "/get",
        params={"user_id": int(user_id), "k": 5}
    )
    
    if response.status_code == 200:
        recent_tracks_list = response.json().get('events', [])
        recent_tracks.write(f"Recent Tracks: {recent_tracks_list}")
    else:
        st.error("Failed to retrieve recent tracks.")

# Поля для вывода рекомендаций
st.header('Recommendation Tracks:')
offline_recs = st.empty()
online_recs = st.empty()
blended_recs = st.empty()

# Функция для добавления имен по id
def add_name(cell_val, col):
    catalog = pd.read_parquet(data_path + '/cat.parquet')
    if isinstance(cell_val, int):
        return catalog.loc[(catalog['type'] == col) & (catalog['id'] == cell_val), 'name'].values[0] if not catalog[(catalog['type'] == col) & (catalog['id'] == cell_val)].empty else None
    elif isinstance(cell_val, object):
        return [catalog.loc[(catalog['type'] == col) & (catalog['id'] == x), 'name'].values[0] for x in cell_val if not catalog[(catalog['type'] == col) & (catalog['id'] == x)].empty]
    else:
        return None

# Получаем реквизиты треков
items = pd.read_parquet(data_path + '/items_frac.parquet')

# Кнопка Play для запуска процесса получения рекомендаций
if st.sidebar.button("Play"):
    try:
        # Получение офлайн-рекомендаций
        offline_response = requests.post(
            recommendations_url + "/recommendations_offline",
            headers=headers,
            params=params
        )
        offline_recommendations = offline_response.json().get('recs', [])
        offline_recs.write(f"Offline Recommendations: {offline_recommendations}")



        # Получение онлайн-рекомендаций
        online_response = requests.post(
            recommendations_url + "/recommendations_online",
            headers=headers,
            params=params
        )
        online_recommendations = online_response.json().get('recs', [])
        online_recs.write(f"Online Recommendations: {online_recommendations}")

        # Получение смешанных рекомендаций (blended)
        blended_response = requests.post(
            recommendations_url + "/recommendations",
            headers=headers,
            params=params
        )
        blended_recommendations = blended_response.json().get('recs', [])
        blended_recs.write(f"Blended Recommendations: {blended_recommendations}")

        blended_recommendations_df = pd.DataFrame(blended_recommendations, columns=['track_id'])
        blended_recommendations_df = blended_recommendations_df.merge(items, on='track_id', how='left')
        blended_recommendations_df['genre'] = blended_recommendations_df['genres'].apply(lambda x: add_name(x, 'genre'))
        blended_recommendations_df['artist'] = blended_recommendations_df['artists'].apply(lambda x: add_name(x, 'artist'))
        blended_recommendations_df['album'] = blended_recommendations_df['albums'].apply(lambda x: add_name(x, 'album'))
        blended_recommendations_df['title'] = blended_recommendations_df['track_id'].apply(lambda x: add_name(x, 'track'))
        blended_recommendations_df = blended_recommendations_df[['track_id','title','artist','album','genre']]
        st.subheader('Blended recommendation list:')
        st.dataframe(blended_recommendations_df)

    except requests.exceptions.RequestException as e:
        st.error(f"An error occurred: {e}")

