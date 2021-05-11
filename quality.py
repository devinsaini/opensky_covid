from pyspark.sql.functions import *


def assert_non_empty(df):
    """Assert that the dataframe df is non empty

    Args:
        df (Dataframe):Input Dataframe to validate
    """
    count = df.count()
    assert count>0
    print(f"record count : {count:7d}, non-empty check passed")

def assert_values(df, column, values):
    """Assert that values are present in a given column of the dataframe df

    Args:
        df (Dataframe): Input Dataframe to validate
        column (string): Dataframe column in which values should be asserted
        values (List[String]): Values to assert in the column
    """
    col_values = [str(row[column]) for row in df.select(col(column)).distinct().collect()]
    for v in values:
        assert v in col_values
    print(f"verified values {values} in column {column}")