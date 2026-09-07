from datetime import datetime
import requests
def preprocess_weather_data(request_date,json_data):
    preprocessed_data = []
    for hourly_data in range(0,24):
        print(hourly_data)
        hourly_preprocessed_data = {
        'requested_date' : request_date,
        'lattitude' : json_data['latitude'],
        'longitude' : json_data['longitude'],
        'elevation' : json_data['elevation'],
        'hour_recorded' : json_data['hourly']['time'][hourly_data],
        'temperature_recorded' : json_data['hourly']['temperature_2m'][hourly_data],
        'generationtime_ms' : json_data["generationtime_ms"],
        'timezone' :  json_data["timezone"],
        'timezone_abbreviation' : json_data["timezone_abbreviation"]}
        preprocessed_data.append(hourly_preprocessed_data)
    return preprocessed_data



request_date = datetime.now().strftime('%Y-%m-%d')
url = f"https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&start_date={request_date}&end_date={request_date}"

response = requests.get(url)
response.raise_for_status()
json_data = response.json()
data_to_store = preprocess_weather_data(request_date,json_data)
print(data_to_store)
