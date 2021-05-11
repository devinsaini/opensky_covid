from pyspark.sql.functions import *


def enrich_flight_data(df_flights, df_airports):
    """Adds airport and flight type data to flights dataframe.

    Args:
        df_flights (Dataframe): Opensky flights dataframe
        df_airports (Dataframe): Airports information dataframe

    Returns:
        Dataframe: Flight dataset enhanced with airport location and flight type
    """

    # enrich flights data with airports info
    df_flights = df_flights.join(broadcast(df_airports.select(
        col('airport').alias('origin'),
        col('country_code').alias('origin_country_code'),
        col('type').alias('origin_airport_type')
    )), 'origin', how='left') \
        .join(broadcast(df_airports.select(
            col('airport').alias('destination'),
            col('country_code').alias('destination_country_code'),
            col('type').alias('destination_airport_type')
        )), 'destination', how='left')

    # classify the flight as domestic or international based on origin and destination countries
    # if either source or destination is None, we assume the flight is domestic
    flight_type_cond = when(col('origin_country_code').isNull() | col('destination_country_code').isNull(), 'domestic') \
        .otherwise(when(col('origin_country_code') == col('destination_country_code'), 'domestic')
                   .otherwise('international'))
    df_flights = df_flights.withColumn('type', flight_type_cond)

    return df_flights


def filter_airport_types(df_flights):
    """Remove flights with airports of type balloonport, seaplane_base, heliport, closed. 
    These types are filtered after joining with flights dataset because we don't want to 
    drop flights that have either origin or destination null.

    Args:
        df_flights (Dataframe): Flights dataframe

    Returns:
        Dataframe: Flight dataset not originating or terminating at balloonport, seaplane_base, heliport, closed airport types.
    """
    # we'll filter flights with origin or destination airports with type balloonport, seaplane_base, heliport, closed.
    # These airport types appear to be more of recreational type and should be irrelevant to our analysis.
    # Small, medium and large airports can also have recreational and charter flights, but we can't identify them.
    ap_types_allowed = ['large_airport', 'medium_airport', 'small_airport']
    df_flights = df_flights \
        .filter(col('origin_airport_type').isin(ap_types_allowed) | col('origin_airport_type').isNull()) \
        .filter(col('destination_airport_type').isin(ap_types_allowed) | col('destination_airport_type').isNull()) \
        .drop('origin_airport_type', 'destination_airport_type')

    return df_flights


def aggregate_flights(df_flights):
    """Aggregates flight data to traffic in and out of airports.

    Args:
        df_flights (Dataframe): Flights data

    Returns:
        Dataframe: Aggregated flight traffic for airports
    """

    # calculate traffic to and from each airport per day
    # for every flight that leaves an origin airport, we can count that as an outbound flight for the airport
    # similarly for destination airport, we count it as inbound
    df_outbound = df_flights \
        .filter(col('origin').isNotNull()) \
        .groupBy(col('date'), col('origin').alias('airport'), col('type')) \
        .agg(count('callsign').alias('outbound'))
    df_inbound = df_flights \
        .filter(col('destination').isNotNull()) \
        .groupBy(col('date'), col('destination').alias('airport'), col('type')) \
        .agg(count('callsign').alias('inbound'))

    # join the inbound and outbound traffic for all airports.
    # notice that since we've filtered origin and destination for nulls, we won't have full join where both are null.
    # pivot the flight type to convert it into columns.
    df_all_traffic = df_outbound.join(df_inbound, ['date', 'airport', 'type'], how='full') \
        .groupBy('date', 'airport') \
        .pivot('type') \
        .agg(first('inbound').alias('inbound'), first('outbound').alias('outbound')) \
        .na.fill(0) \
        .cache()

    return df_all_traffic


def flights_transform(df_flights, df_airports):
    """Enhances, filters and aggregates flight data to calculate airport traffic.

    Args:
        df_flights (Dataframe): Flights data
        df_airports (Dataframe): Airports data

    Returns:
        Dataframe: Airport traffic data
    """
    df_flights = enrich_flight_data(df_flights, df_airports)
    df_flights = filter_airport_types(df_flights)
    df_traffic = aggregate_flights(df_flights)
    return df_traffic


def airports_transform(df_airports, df_iban):
    """Merges airport data with country code data

    Args:
        df_airports (Dataframe): Airports data
        df_iban (Dataframe): Country code data

    Returns:
        Dataframe: Airports data with country information
    """
    return df_airports.join(broadcast(df_iban), 'country_code')


def covid_data_transform_fact(df_covid):
    """Transforms COVID dataset to fact table

    Args:
        df_covid (Dataframe): COVID case data

    Returns:
        Dataframe: COVID fact table
    """
    return df_covid.select('date', 'country', 'province_state', 'admin2', 'confirmed', 'deaths', 'recovered')


def covid_data_transform_dim(df_covid):
    """Transforms COVID data to location dimension table

    Args:
        df_covid (Dataframe): COVID dataset

    Returns:
        Dataframe: COVID dimension table containing location information
    """
    return df_covid.select('country', 'province_state', 'admin2', 'latitude', 'longitude') \
        .dropDuplicates(['country', 'province_state', 'admin2'])
