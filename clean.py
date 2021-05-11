from pyspark.sql.functions import *


def clean_flights(df_os_raw):
    """Clean flights data

    Args:
        df_os_raw (Dataframe): Raw OpenSky flights dataset

    Returns:
        Dataframe: Flights data with relevant columns only
    """
    df = df_os_raw.select(
        col('callsign'), 
        col('origin'), 
        col('destination'), 
        to_date(col('day')).alias('date')
    )
    return df

def clean_airports(df_airports_raw):
    """Cleans airport dataset

    Args:
        df_airports_raw (Dataframe): Raw airports dataset

    Returns:
        Dataframe: cleaned airports dataset
    """
    df = df_airports_raw \
        .select(
            col('ident').alias('airport'), 
            col('type'), 
            col('name'), 
            col('iso_country').alias('country_code'), 
            col('iso_region').alias('region'), 
            col('continent'), 
            col('coordinates')
        )
    return df

def clean_country_codes(df_iban):
    """Cleans country codes dataset

    Args:
        df_iban (Dataframe): Raw country code dataset

    Returns:
        Dataframe: dataset with columns renamed appropriately
    """
    df = df_iban \
        .select(
            col('Code').alias('country_code'),
            col('Name').alias('country')
        )
    return df

def clean_covid_data(df_covid_raw):
    """Cleans JHU COVID dataset

    Args:
        df_covid_raw (Dataframe): Raw JHU Enigma COVID dataset

    Returns:
        Dataframe: cleaned COVID dataset
    """
    df_covid = df_covid_raw.select(
        to_date(col('date')).alias('date'),
        col('country_region').alias('country'),
        col('province_state'),
        col('admin2'),
        col('latitude'),
        col('longitude'),
        col('confirmed'),
        col('deaths'),
        col('recovered')
    ).na.fill(0)
    return df_covid