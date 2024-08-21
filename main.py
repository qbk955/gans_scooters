import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import sqlalchemy
from pytz import timezone
from lat_lon_parser import parse
from sqlalchemy import create_engine
import pymysql
import functions_framework
from passwords import mysql_password, weather_api, flights_api
@functions_framework.http
def hopeitworks(request):
    try:
        connection_string = create_connection_string()
        print("Connection string created successfully.")
        
        cities_df = fetch_cities_data(connection_string)
        print(f"Fetched cities data: {cities_df.head()}")
        
        weather_df = fetch_weather_data(cities_df)
        print(f"Fetched weather data: {weather_df.head()}")
        
        store_weather_data(weather_df, connection_string)
        print("Weather data stored successfully.")
        
        icao_list = fetch_airport_icao(connection_string)
        print(f"Fetched ICAO list: {icao_list}")
        
        list_for_arrivals_df = tomorrows_flight_arrivals(icao_list)
        print(f"Fetched flights data: {list_for_arrivals_df.head() if not list_for_arrivals_df.empty else 'No data available'}")
        
        store_flight_data(list_for_arrivals_df, connection_string)
        print("Flight data stored successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return "good is working"

def create_connection_string(schema="gans_gcp", 
                          host="34.76.135.146", 
                          user="root", 
                          password= mysql_password, 
                          port=3306):
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string

def fetch_cities_data(connection_string):
  return pd.read_sql("cities", con=connection_string)

def fetch_weather_data(cities_df):
  berlin_timezone = timezone('Europe/Berlin')
  API_key = weather_api
  weather_items = []

  for _, city in cities_df.iterrows():
      latitude = city["Latitude"]
      longitude = city["Longitude"]
      city_id = city["City_id"]

      url = (f"https://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid={API_key}&units=metric")
      response = requests.get(url)
      weather_data = response.json()

      retrieval_time = datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

      for item in weather_data["list"]:
          weather_item = {
              "city_id": city_id,
              "forecast_time": item.get("dt_txt"),
              "temperature": item["main"].get("temp"),
              "rain_in_last_3h": item.get("rain", {}).get("3h", 0),
              "wind_speed": item["wind"].get("speed"),
              "data_retrieved_at": retrieval_time
          }
          weather_items.append(weather_item)

  weather_df = pd.DataFrame(weather_items)
  weather_df["forecast_time"] = pd.to_datetime(weather_df["forecast_time"])
  weather_df["data_retrieved_at"] = pd.to_datetime(weather_df["data_retrieved_at"])

  return weather_df
 
def store_weather_data(weather_df, connection_string):
    weather_df.to_sql('weather',
                    if_exists='append',
                    con=connection_string,
                    index=False)

def fetch_airport_icao(connection_string):
    
    engine = create_engine(connection_string)
    
    
    query = "SELECT icao FROM airports"
    
    
    icao_df = pd.read_sql(query, con=engine)
    
    
    icao_list = icao_df['icao'].tolist()
    
    return icao_list

def tomorrows_flight_arrivals(icao_list):

    api_key = flights_api

    berlin_timezone = timezone('Europe/Berlin')
    today = datetime.now(berlin_timezone).date()
    tomorrow = (today + timedelta(days=1))

    list_for_arrivals_df = []

    for icao in icao_list:

        times = [["00:00","11:59"],["12:00","23:59"]]

        for time in times:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}"

            querystring = {"direction":"Arrival","withCancelled":"false"}

            headers = {
                "X-RapidAPI-Key": flights_api,
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
                }

            response = requests.request("GET", url, headers=headers, params=querystring)
            flights_resp = response.json()

            arrivals_df = pd.json_normalize(flights_resp["arrivals"])[["number", "airline.name", "movement.scheduledTime.local", "movement.terminal", "movement.airport.name", "movement.airport.icao"]]
            arrivals_df = arrivals_df.rename(columns={"number": "flight_number", "airline.name": "airline", "movement.scheduledTime.local": "arrival_time", "movement.terminal": "arrival_terminal", "movement.airport.name": "departure_city", "movement.airport.icao": "departure_airport_icao"})
            arrivals_df["arrival_airport_icao"] = icao
            arrivals_df["data_retrieved_on"] = datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
            arrivals_df = arrivals_df[["arrival_airport_icao", "flight_number", "airline", "arrival_time", "arrival_terminal", "departure_city", "departure_airport_icao", "data_retrieved_on"]]

            
            arrivals_df["arrival_time"] = arrivals_df["arrival_time"].str.split("+").str[0]

            list_for_arrivals_df.append(arrivals_df)

    return pd.concat(list_for_arrivals_df, ignore_index=True) 
 
def store_flight_data(flights_df, connection_string):
    flights_df.to_sql('flights',
                  if_exists='append',
                  con=connection_string,
                  index=False)