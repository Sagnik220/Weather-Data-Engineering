# importing necessary libraries
import io
import json
import logging
import http.client
import pandas as pd
import oci
import os
import ocifs
from fdk import response
from datetime import date




# Function to GET weather data based on Latitude & Longitude in
def get_weather_data(latitude, longitude, api_key):
    conn = http.client.HTTPSConnection("weatherapi-com.p.rapidapi.com")
    headers = {
        'X-RapidAPI-Key': api_key.strip(),
        'X-RapidAPI-Host': "weatherapi-com.p.rapidapi.com"
    }
    query = f"/current.json?q={latitude}%2C{longitude}"
    conn.request("GET", query, headers=headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def weather_data_extraction():
    with open("api_key.txt", "r") as file:
        api_key = file.read().strip()
    states = {
        # Your list of states
    "Andhra Pradesh": (17.385044, 78.486671),
    "Arunachal Pradesh": (27.0880, 93.6958),
    "Assam": (26.1433, 91.7898),
    "Bihar": (25.5941, 85.1376),
    "Chhattisgarh": (21.2514, 81.6296),
    "Goa": (15.2993, 74.1240),
    "Gujarat": (23.2156, 72.6369),
    "Haryana": (30.7333, 76.7794),
    "Himachal Pradesh": (31.1048, 77.1734),
    "Jharkhand": (23.3441, 85.3096),
    "Karnataka": (12.9716, 77.5946),
    "Kerala": (8.5241, 76.9366),
    "Madhya Pradesh": (23.2599, 77.4126),
    "Maharashtra": (19.0760, 72.8777),
    "Manipur": (24.8170, 93.9368),
    "Meghalaya": (25.5788, 91.8933),
    "Mizoram": (23.7271, 92.7176),
    "Nagaland": (25.6751, 94.1086),
    "Odisha": (20.2961, 85.8245),
    "Punjab": (30.7333, 76.7794),
    "Rajasthan": (26.9124, 75.7873),
    "Sikkim": (27.3389, 88.6065),
    "Tamil Nadu": (13.0827, 80.2707),
    "Telangana": (17.385044, 78.486671),
    "Tripura": (23.8315, 91.2868),
    "Uttar Pradesh": (26.8467, 80.9462),
    "Uttarakhand": (30.3165, 78.0322),
    "West Bengal": (22.5726, 88.3639)
    }
    weather_data_list = []
    for state, coordinates in states.items():
        latitude, longitude = coordinates
        weather_data = get_weather_data(latitude, longitude, api_key)
        try:
            json_data = json.loads(weather_data)
            location = json_data["location"]
            current = json_data["current"]
            weather_data_list.append({
                "State": state,
                "City": location["name"],
                "Region": location["region"],
                "Country": location["country"],
                "Temperature_Celsius": current["temp_c"],
                "Humidity": current["humidity"],
                "Real_Feel_Celsius": current["feelslike_c"],
                "Wind_Speed_Kph":current["wind_kph"],
                "Wind_Degree":current["wind_degree"],
                "Wind_Direction":current["wind_dir"],
                "Wind_Pressure_mb":current["pressure_mb"],
                "Wind_Pressure_in":current["pressure_in"],
                "Cloud":current["cloud"],
                "Vis_km":current["vis_km"],
                "Vis_miles":current["vis_miles"],
                "UV":current["uv"],
                "Gust_mph":current["gust_mph"],
                "Gust_kph":current["gust_kph"],
                "Date_of_Extraction":date.today()
            })
        except (KeyError, json.JSONDecodeError):
            print(f"Error parsing weather data for {state}")
    df = pd.DataFrame(weather_data_list)
    try:
        csv_data = df.to_csv(index=False)
    except Exception as e:
        print("Error converting DataFrame to CSV:", str(e))
        csv_data = None

    signer = oci.auth.signers.get_resource_principals_signer()
    object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)
    oci_namespace = 'gc35013'
    oci_bucket_name = 'Extracted_Weather_Data'
    object_name = 'weather_data.csv'

    try:
        existing_object = object_storage.get_object(oci_namespace, oci_bucket_name, object_name)
        existing_etag = existing_object.headers["ETag"].strip('"')
        object_storage.put_object(oci_namespace, oci_bucket_name, object_name, csv_data.encode('utf-8'), if_match=existing_etag)
        print("CSV file replaced in Oracle Object Storage.")
    except oci.exceptions.ServiceError:
        object_storage.put_object(oci_namespace, oci_bucket_name, object_name, csv_data.encode('utf-8'))
        
    return ("CSV file uploaded to Oracle Object Storage.")

def handler(ctx, data: io.BytesIO = None):
    try:
        data_extraction=weather_data_extraction()
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    logging.getLogger().info("Data Extraction Function")
    return response.Response(
        ctx, response_data=json.dumps(
            {"message":data_extraction}),
        headers={"Content-Type": "application/json"}
    )
