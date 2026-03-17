# data manipulation
import pandas as pd
import numpy as np

# API endpoints
from data_ingestion import request_openf1_data

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
        "sessions", year=year_, circuit_short_name=circuit_name_
    )

    # Identify race
    race_session = sessions[sessions["session_name"] == "Race"]
    if race_session.empty:
        raise ValueError("Race session not found")

    session_key = race_session.iloc[0]["session_key"]
    print(f"Found session_key: {session_key}")

    return session_key

def fetch_driver_telemetry(session_key_, driver_number_):
    """Fetch driver telemetry for the session

    Args:
        session_key_ (int): Session ID
        driver_number_ (int): Driver Number

    Returns:
        pd.DataFrame: Car telemetry data
    """

    telemetry = request_openf1_data(
        "car_data",
        session_key=session_key_,
        driver_number=driver_number_
    )

    df = pd.DataFrame(telemetry)

    if df.empty:
        return df

    df["driver_number"] = driver_number_
    df["date"] = pd.to_datetime(df["date"], format='ISO8601')

    return df

def download_and_process_telemetry(session_key, driver_numbers, laps):
    """
    Download car telemetery data in batches (per driver) and process into the 
    required format

    Args:
        session_key (int): Grand Prix Session key
        driver_numbers (list): List of drivers participating in the session
        laps (pd.DataFrame): Lap data

    Returns:
        pd.DataFrame: Processed Car Telemetry data
    """
    
    # columns for telemetry
    columns = [
        "date", "driver_number", "lap_number", "micro_sector", 
        "speed", "throttle", "TimeSeconds", "LapTimeSeconds"
    ]
    
    # ensure lap has start time and end time 
    laps["date_start"] = pd.to_datetime(laps["date_start"], format='ISO8601')
    laps["date_end"] = (
        laps["date_start"] + pd.to_timedelta(laps["lap_duration"], unit="s")
    )

    all_telemetry = []

    for driver in driver_numbers:
        print(f"Downloading telemetry for driver {driver}")

        # fetch driver telemetry
        df = fetch_driver_telemetry(
            session_key,
            driver
        )

        if df.empty:
            print(f"Driver DNS. No data.")
            continue
        
        print("Assigning Laps to Telemetry...")
        df = assign_laps_to_telemetry(df, laps)

        print("Building micro-sectors and aggregate...")
        df = build_microsectors(df)
        df = aggregate_microsectors(df)
        
        # append data to all telemetry dataframe
        all_telemetry.append(df[columns])

    telemetry = pd.concat(
        all_telemetry,
        ignore_index=True
    )

    return telemetry

def aggregate_microsectors(df):
    #todo: improve aggregation estimates
    grouped = df.groupby(
        ["driver_number", "lap_number", "micro_sector"],
        as_index=False
    ).agg({
        "date" : "min",
        "TimeSeconds": "sum",
        "LapTimeSeconds": "mean",
        # "distance_ratio": "sum",
        # "cum_distance": "max",
        "speed": "mean",
        "throttle": "mean",
    })

    return grouped.reset_index().drop(columns=["index"])

def build_microsectors(telemetry):
    """
    Build microsectors from the telemetry data. A single lap has three major 
    sector. We want to divide the circuit into 100 equi-distant sectors.

    Args:
        telemetry (pd.DataFrame): Telemetry data

    Returns:
        pd.DataFrame: Micro sector telemetry data
    """
    def update_first_element(group):
        """
        Update first element of difference in time seconds.
        """
        # Sum all elements in 'TimeSeconds' except for the first one (index 0)
        remaining_sum = group['TimeSeconds'].iloc[1:].sum()
        
        # Calculate the new value for the first element
        # New Value = lap_duration - sum(remaining elements)
        group.iloc[0, group.columns.get_loc('TimeSeconds')] = group['LapTimeSeconds'].iloc[0] - remaining_sum
        return group
    
    telemetry = telemetry.copy()

    # Ensure proper ordering
    telemetry["date"] = pd.to_datetime(telemetry["date"], format='ISO8601')
    telemetry = telemetry.sort_values(
        ["driver_number", "lap_number", "date"]
    )

    # Time difference between telemetry points
    groups = telemetry.groupby(["driver_number", "lap_number"])
    telemetry["TimeSeconds"] = (
        telemetry.groupby(["driver_number", "lap_number"])["date"]
        .diff()
        .dt.total_seconds()
    )
    telemetry["TimeSeconds"] = (
        telemetry.groupby(["driver_number", "lap_number"], group_keys=False)
        .apply(update_first_element)
    )

    # Convert speed to meters per second
    telemetry["speed_mps"] = telemetry["speed"] / 3.6

    # Distance traveled between samples
    telemetry["delta_distance"] = (
        telemetry["speed_mps"] * telemetry["TimeSeconds"]
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

def assign_laps_to_telemetry(telemetry, laps):
    """
    Assign lap numbers and times to car telemetry data

    Args:
        telemetry (pd.DataFrame): Car telemetry data
        laps (pd.DataFrame): Lap data

    Returns:
        pd.DataFrame: Car telemetry data with lap numbers
    """

    driver = telemetry["driver_number"].iloc[0]
    driver_laps = laps[laps["driver_number"] == driver]
    telemetry["lap_number"] = None

    for _, lap in driver_laps.iterrows():

        mask = (
            (lap["date_start"] <= telemetry["date"]) &
            (telemetry["date"] <= lap["date_end"])
        )

        # assign lap number and times
        telemetry.loc[mask, "lap_number"] = lap["lap_number"]
        telemetry.loc[mask, "LapTimeSeconds"] = lap["lap_duration"]

    telemetry = telemetry.dropna(subset=["lap_number"])
    telemetry["lap_number"] = telemetry["lap_number"].astype(int)

    return telemetry

def download_race_data(session_key_):
    """
    Download lap, stint, pit, weather and driver data

    Args:
        session_key_ (int): Race session ID
    """

    laps = request_openf1_data("laps", session_key=session_key_)    
    stints = request_openf1_data("stints", session_key=session_key_)
    pits = request_openf1_data("pit", session_key=session_key_)
    drivers = request_openf1_data("drivers", session_key=session_key_)
    weather = request_openf1_data("weather", session_key=session_key_)
    race_control = request_openf1_data("race_control", session_key=session_key_)

    return laps, stints, pits, weather, drivers, race_control

def merge_weather(df, weather):
    """
    Merge Dataframe and Weather Data on date

    Args:
        df (pd.DataFrame): Telemetry data
        weather (pd.DataFrame): Weather data

    Returns:
        pd.DataFrame: Merged data
    """

    weather["date"] = pd.to_datetime(weather["date"], format='ISO8601')
    weather = weather[["date", "air_temperature"]] # select columns
    
    df["date"] = pd.to_datetime(df["date"], format='ISO8601')

    df = pd.merge_asof(
        df.sort_values("date"),
        weather.sort_values("date"),
        on="date",
        direction="nearest"
    )
    
    df = df.rename(columns={
        "air_temperature": "AirTemp"
    })

    return df

def merge_drivers(df, drivers):
    """
    Merge dataframe and driver data

    Args:
        df (pd.DataFrame): Main data
        drivers (pd.DataFrame): Drivers data

    Returns:
        pd.DataFrame: Merged data
    """

    # select required data
    drivers = drivers[["driver_number", "name_acronym", "team_name" ]]
    df = df.merge(drivers, on="driver_number", how="left")
    df = df.rename(columns={
        "name_acronym": "Driver",
        "team_name": "Team"
    })

    return df.drop(columns=["driver_number"])

def map_track_status(category, flag):
    """
    Identify racing conditions based on the directive issued by the race
    control

    Args:
        category (str): The category of the event (SessionStatus, CarEvent, Drs, Flag, SafetyCar, ...).
        flag (str): Type of flag displayed (GREEN, YELLOW, DOUBLE YELLOW, CHEQUERED, ...).

    Returns:
        _type_: _description_
    """
    category = category.upper()
    flag = flag.upper()

    if ("SAFETYCAR" in category) or (("FLAG" in category) and (flag == "GREEN")):
        return 0
    return 1

def expand_stints_to_laps(stints):

    rows = []

    for _, row in stints.iterrows():

        for lap in range(row["lap_start"], row["lap_end"] + 1):

            rows.append({
                "driver_number": row["driver_number"],
                "lap_number": lap,
                "stint_number": row["stint_number"],
                "compound": row["compound"],
                
                # tyre life increases each lap
                "TyreLife": row["tyre_age_at_start"] + (lap - row["lap_start"])
            })

    return pd.DataFrame(rows)

def merge_stints(df, stints):
    """
    Merge dataframe and stint data

    Args:
        df (pd.DataFrame): Main data
        stints (pd.DataFrame): Stints data

    Returns:
        pd.DataFrame: Merged data
    """

    stints = stints[[
        "driver_number", "stint_number", "lap_start", "lap_end",
        "compound", "tyre_age_at_start"
    ]]
    # expand to stints
    stints = expand_stints_to_laps(stints)
    # merge stint and laps data
    df = df.merge(
        stints, on=["driver_number", "lap_number"], how="left"
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

def build_datasets(year, circuit_name):
    """
    Data ingestion pipeline

    Args:
        year (int): Year of F1 Grand Prix
        circuit_name (str): F1 Grand Prix Circuit Name

    Returns:
        pd.DataFrame: Race Dataset
    """

    session_key = get_race_session(year_=year, circuit_name_=circuit_name)

    laps, stints, pits, weather, drivers, rc = download_race_data(session_key_=session_key)

    # ------------------------------------------------------------------------
    # Build Laps dataset - race conditions
    # ------------------------------------------------------------------------
    print("Building Lap dataset...")
    print("Merging stint data...")
    laps_df = merge_stints(laps, stints)
    
    print("Merging drivers to lap data...")
    laps_df = merge_drivers(laps_df, drivers)
    
    print("Merging drivers...")
    laps_df = merge_drivers(laps_df, drivers)
    
    # ------------------------------------------------------------------------
    # Build Micro-sector telemetry dataset
    # ------------------------------------------------------------------------
    print("Building Micro-sector telemetry dataset...")
    
    # get laps start and end data
    telemetry = download_and_process_telemetry(
        session_key=session_key, drivers=drivers["driver_number"], laps=laps
    )
    
    print("Merging weather...")
    telemetry_df = merge_weather(telemetry, weather)
    
    print("Merging drivers to micro-sector telemetry...")
    telemetry_df = merge_drivers(telemetry_df, drivers)

    return laps_df, telemetry_df