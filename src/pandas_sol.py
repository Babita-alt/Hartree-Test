import pandas as pd
from itertools import combinations
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs.log"),
        logging.StreamHandler()
    ]
)

# Create a logger
logger = logging.getLogger("Pandas Solution")


def merge_df(data1: pd.DataFrame, data2: pd.DataFrame, column_bases: str = "counter_party", join_type: str = "outer") -> pd.DataFrame:
    """
    Merge two DataFrames based on a common column using a specified join type.

    Parameters:
        data1 (pd.DataFrame): The first DataFrame to be merged.
        data2 (pd.DataFrame): The second DataFrame to be merged.
        column_bases (str): The column to merge on (default is "counter_party").
        join_type (str): The type of join to perform (default is "outer").

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    logger.info(
        f"Merging DataFrames on '{column_bases}' using a '{join_type}' join.")
    return pd.merge(data1, data2, on=column_bases, how=join_type)

# function to make different pairs of columns.


def generate_group_keys(cols: list) -> list[tuple[list[str], list[str]]]:
    """
    Generate different combinations of columns to group by.

    Parameters:
        cols (list): List of column names.

    Returns:
        list[tuple[list[str], list[str]]: List of tuples, each containing two lists.
    """
    group_keys = []
    for i in range(1, len(cols)):
        for item in combinations(cols, i):
            remaining_columns = [col for col in cols if col not in item]
            if remaining_columns == ["counter_party"] or remaining_columns == ["legal_entity"]:
                break
            group_keys.append((list(item), remaining_columns))
    return group_keys

# function to aggregate data based on group by using different columns.


def aggregate(df: pd.DataFrame, columns_to_group_by: list[str], remaining_columns: list[str], column_order: tuple[str]) -> pd.DataFrame:
    """
    Aggregate data based on specified columns to group by.

    Parameters:
        df (pd.DataFrame): The input DataFrame to perform aggregation on.
        columns_to_group_by (list): List of columns to group by.
        remaining_columns (list): List of columns to aggregate.
        column_order (list): List of column order in the resulting DataFrame.

    Returns:
        pd.DataFrame: The aggregated DataFrame.
    """
    logger.info(
        f"Aggregating data based on group by columns: {columns_to_group_by}")
    aggregation_rules = {f"{col}": (col, "count") for col in remaining_columns}
    aggregation_rules.update(
        calculate_value(df, "status", "ARAP", "ACCR", "rating", "value")
    )
    df_new = df.groupby(columns_to_group_by).agg(
        **aggregation_rules).reset_index()
    return df_new[list(column_order)]

# function to calculate the values based on different conditions.


def calculate_value(
        df: pd.DataFrame, condition_column: str, condition1: str, condition2: str, maximum_column: str, sum_column: str) -> dict:
    """
    Define aggregation rules for specific conditions.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        condition_column (str): The column used for conditions.
        condition1 (str): The first condition.
        condition2 (str): The second condition.
        maximum_column (str): The column for calculating the maximum.
        sum_column (str): The column for calculating the sum.

    Returns:
        dict: A dictionary of aggregation rules.
    """
    logger.info(
        f"Calculating values based on conditions '{condition1}' and '{condition2}'")
    aggregation_dict = {}
    aggregation_dict[f"total_of_{sum_column}_{condition1}"] = (
        sum_column,
        lambda x: x[df[condition_column] == condition1].sum(),
    )
    aggregation_dict[f"total_of_{sum_column}_{condition2}"] = (
        sum_column,
        lambda x: x[df[condition_column] == condition2].sum(),
    )
    aggregation_dict[f"max_of_{maximum_column}"] = (maximum_column, "max")
    return aggregation_dict


def combine_df(df: pd.DataFrame, keys: list[tuple[list[str], list[str]]], data1: pd.DataFrame, data2: pd.DataFrame, column_order: tuple[str]) -> pd.DataFrame:
    """
    Combine and aggregate data based on specified keys and columns.

    Parameters:
        df (pd.DataFrame): The input DataFrame for aggregation.
        keys (list[tuple[list[str], list[str]]): List of key pairs.
        data1 (pd.DataFrame): The first data source.
        data2 (pd.DataFrame): The second data source.
        column_order (list): List of column order in the resulting DataFrame.

    Returns:
        pd.DataFrame: The combined and aggregated DataFrame.
    """
    logger.info("Combining and aggregating data based on keys and columns.")
    combined_data = []
    for columns_to_group_by, remaining_columns in keys:
        df_final = aggregate(df, columns_to_group_by,
                             remaining_columns, column_order)
        if len(columns_to_group_by) == 1:
            for i in remaining_columns:
                df_final[i] = "Total"
        for index, data in df_final.iterrows():
            if data["tier"] != "Total" and data["counter_party"] != "Total":
                ser = data2.query(
                    f"counter_party=='{data['counter_party']}'")['tier']
                if not ser.empty:
                    df_final.at[index, "tier"] = int(ser.iloc[0])
        combined_data.append(df_final)
    return pd.concat(combined_data, axis=0, ignore_index=True)


def save_to(df: pd.DataFrame, output_path: str = "outputs/pandas_output.csv") -> None:
    """
    Save a DataFrame to a CSV file.

    Parameters:
        output (pd.DataFrame): The DataFrame to be saved.
        output_path (str): The path to the output CSV file (default is "outputs/pandas_output.csv").

    Returns:
        None
    """
    logger.info(f"Saving the DataFrame to '{output_path}'.")
    df.to_csv(output_path, index=False)


if __name__ == "__main__":

    data1_path = "inputs/dataset1.csv"
    data2_path = "inputs/dataset2.csv"
    data1 = pd.read_csv(data1_path)
    data2 = pd.read_csv(data2_path)
    GROUP_BY_COLUMNS = ("legal_entity", "counter_party", "tier")
    COLUMN_ORDER = (
        "legal_entity",
        "counter_party",
        "tier",
        "max_of_rating",
        "total_of_value_ARAP",
        "total_of_value_ACCR",
    )

    outfile_path = "outputs/pandas_output.csv"
    keys = generate_group_keys(GROUP_BY_COLUMNS)
    df = merge_df(data1, data2)
    output_df = combine_df(df, keys, data1, data2, COLUMN_ORDER)
    save_to(output_df, outfile_path)
