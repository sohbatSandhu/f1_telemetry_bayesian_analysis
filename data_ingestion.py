# api
import requests
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
import json

# data manipulation
import pandas as pd
import numpy as np

# OpenF1 API url
BASE_URL = "https://api.openf1.org/v1"

def request_openf1_data(endpoint, **params) -> pd.DataFrame:
    """
    Request data from OpenF1 API using a endpoint and the respective parameters
    passed 

    Args:
        endpoint (str): API endpoint resource path

    Returns:
        pd.DataFrame: Data retrieved
    """
    query = urlencode(params, doseq=True)
    url = f"{BASE_URL}/{endpoint}?{query}" # base url w/ endpoint and query
    data = None
    
    print("Requesting:", url)
    try:
        with urlopen(url) as response:
            # For successful responses (e.g., 200)
            response_status = response.getcode() # Or response.status
            data = json.loads(response.read().decode("utf-8"))
            print(f"Success! Response code: {response_status}")
            return pd.DataFrame(data)
    except HTTPError as e:
        # For HTTP errors (e.g., 404, 500)
        response_status = e.code
        print(f"HTTP Error! Response code: {response_status}")
    except URLError as e:
        # For other URL-related errors (e.g., connection issues)
        print(f"URL Error! Reason: {e.reason}")
    
    return None

def get_race_session(year_, circuit_name_):
    """
    Get race session ID for Grand Prix of year and location

    Args:
        year_ (int): Year of Grand Prix
        circuit_name_ (str): Circuit Name

    Raises:
        ValueError: If race session is not found

    Returns:
        int: Unique session key
    """
    # get session data
    sessions = request_openf1_data(
        "sessions", year=year_,circuit_short_name=circuit_name_
    )

    # Identify race
    race_session = sessions[sessions["session_name"] == "Race"]
    if race_session.empty:
        raise ValueError("Race session not found")

    session_key = race_session.iloc[0]["session_key"]
    print(f"Found session_key: {session_key}")

    return session_key

def download_race_data(session_key_):
    """
    Download lap, car telemetry, weather and driver data

    Args:
        session_key_ (int): Race session ID
    """

    laps = request_openf1_data("laps", session_key=session_key_)
    telemetry = request_openf1_data("car_data", session_key=session_key_)
    weather = request_openf1_data("weather", session_key=session_key_)
    drivers = request_openf1_data("drivers", session_key=session_key_)

    return laps, telemetry, weather, drivers

def build_microsectors(telemetry):
    """
    Build microsectors from the telemetry data. A single lap has three major 
    sector. We want to divide the circuit into 100 equi-distant sectors.

    Args:
        telemetry (pd.DataFrame): Telemetry data

    Returns:
        _type_: _description_
    """
    telemetry = telemetry.copy()

    # Ensure proper ordering
    telemetry["date"] = pd.to_datetime(telemetry["date"])
    telemetry = telemetry.sort_values(
        ["driver_number", "lap_number", "date"]
    )

    # Time difference between telemetry points
    telemetry["delta_t"] = (
        telemetry
        .groupby(["driver_number", "lap_number"])["date"]
        .diff()
        .dt.total_seconds()
    )

    # Convert speed to meters per second
    telemetry["speed_mps"] = telemetry["speed"] / 3.6

    # Distance traveled between samples
    telemetry["delta_distance"] = (
        telemetry["speed_mps"] * telemetry["delta_t"]
    )

    telemetry["delta_distance"] = telemetry["delta_distance"].fillna(0)

    # Cumulative distance for each lap
    telemetry["cum_distance"] = (
        telemetry
        .groupby(["driver_number", "lap_number"])["delta_distance"]
        .cumsum()
    )

    # Total lap distance estimate
    lap_distance = telemetry.groupby(
        ["driver_number", "lap_number"]
    )["cum_distance"].transform("max")

    # Normalize distance to [0,1]
    telemetry["distance_ratio"] = (
        telemetry["cum_distance"] / lap_distance
    )

    # Assign micro-sectors (0–99)
    telemetry["micro_sector"] = (
        telemetry["distance_ratio"] * 100
    ).astype(int)

    telemetry["micro_sector"] = telemetry["micro_sector"].clip(0, 99)

    return telemetry

def merge_weather(telemetry, weather):
    """
    Merge Telemetry and Weather Data

    Args:
        telemetry (pd.DataFrame): Telemetry data
        weather (pd.DataFrame): Weather data

    Returns:
        pd.DataFrame: Merged data
    """

    weather["date"] = pd.to_datetime(weather["date"])
    telemetry["date"] = pd.to_datetime(telemetry["date"])

    telemetry = pd.merge_asof(
        telemetry.sort_values("date"),
        weather.sort_values("date"),
        on="date",
        direction="nearest"
    )

    return telemetry

def merge_drivers(df, drivers):
    """
    Merge dataframe and driver data

    Args:
        df (pd.DataFrame): Main data
        drivers (pd.DataFrame): Drivers data

    Returns:
        pd.DataFrame: Merged data
    """

    drivers = drivers[["driver_number", "name_acronym", "team_name" ]]
    df = df.merge(drivers, on="driver_number", how="left")
    df = df.rename(columns={
        "name_acronym": "Driver",
        "team_name": "Team"
    })

    return df

def merge_laps(df, laps):
    """
    Merge dataframe and lap data

    Args:
        df (pd.DataFrame): Main data
        laps (pd.DataFrame): Lap

    Returns:
        pd.DataFrame: Merged data
    """

    laps = laps[[
        "driver_number", "lap_number", "lap_time", "stint",
        "tyre_age", "compound", "track_status"
    ]]

    df = df.merge(
        laps, on=["driver_number", "lap_number"], how="left"
    )

    return df

def finalize_dataset(df):
    """
    Finalize data for analysis

    Args:
        df (pd.DataFrame): Initial dataset

    Returns:
        pd.DataFrame: Final dataset
    """
    df = df.rename(columns={
        "speed": "Speed",
        "throttle": "Throttle",
        "air_temperature": "AirTemp",
        "tyre_age": "TyreLife"
    })

    columns = [
        "Driver",
        "Team",
        "driver_number",
        "lap_number",
        "micro_sector",
        "Speed",
        "Throttle",
        "AirTemp",
        "TyreLife",
        "compound",
        "lap_time",
        "stint",
        "track_status"
    ]

    df = df[columns]

    return df

def build_race_dataset(year, circuit_name):
    """
    Data ingestion pipeline

    Args:
        year (int): Year of F1 Grand Prix
        circuit_name (str): F1 Grand Prix Circuit Name

    Returns:
        pd.DataFrame: Race Dataset
    """

    session_key = get_race_session(year_=year, circuit_name_=circuit_name)

    laps, telemetry, weather, drivers = download_race_data(session_key_=session_key)
    print(telemetry)
    import sys; sys.exit()

    print("Building micro-sectors...")
    telemetry = build_microsectors(telemetry)

    print("Merging weather...")
    df = merge_weather(telemetry, weather)

    print("Merging drivers...")
    df = merge_drivers(df, drivers)

    print("Merging laps...")
    df = merge_laps(df, laps)

    print("Finalizing dataset...")
    df = finalize_dataset(df)

    return df

if __name__ == "__main__":
    # Grand Prix Parameters
    gp_year = 2023
    gp_circuit_name = "Yas Marina Circuit"
    

    df = build_race_dataset(
        year=gp_year, circuit_name=gp_circuit_name
    )

    df.to_csv("data/main/", index=False)