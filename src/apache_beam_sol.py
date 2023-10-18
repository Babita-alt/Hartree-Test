import apache_beam as beam
import typing
import logging
import sys

# Configure the logging settings
logger = logging.getLogger('apache_beam_sol')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

class InvoiceItem(typing.NamedTuple):
    """
    Represents an invoice record with attributes.

    Attributes:
        legal_entity (str): The legal entity associated with the invoice.
        counter_party (str): The counter party associated with the invoice.
        rating (int): The rating of the invoice.
        status (str): The status of the invoice (e.g., "ARAP").
        value (int): The value of the invoice.
        tier (int): The tier of the invoice.
    """
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: int
    tier: int

class Result(typing.NamedTuple):
    """
    Represents the result of data processing with attributes.

    Attributes:
        legal_entity (any): The legal entity or "Total" for aggregated results.
        counter_party (any): The counter party or "Total" for aggregated results.
        tier (any): The tier or "Total" for aggregated results.
        rating (int): The maximum rating.
        ARAP (int): The total value for invoices with status "ARAP."
        ACCR (int): The total value for invoices with status "ACCR."
    """
    legal_entity: any
    counter_party: any
    tier: any
    rating: int
    ARAP: int
    ACCR: int

def process_invoice(element, type):
    """
    Process and aggregate invoice data to generate a Result.

    Args:
        element: An element representing a group of invoices.
        type: An integer indicating the type of aggregation to perform.

    Returns:
        Result: The aggregated result.
    """
    # Aggregate values and find the maximum rating
    max_rating = 0
    sum_ARAP = 0
    sum_ACCR = 0
    for row in element[1]:
        if row.status == "ARAP":
            sum_ARAP += row.value
        else:
            sum_ACCR += row.value
        max_rating = max(max_rating, row.rating)

    # Generate the Result object
    return Result(
        legal_entity=element[1][0].legal_entity if type == 1 or type == 2 else "Total",
        counter_party=element[1][0].counter_party if type == 2 or type == 3 else "Total",
        tier=element[1][0].tier if type == 4 else "Total",
        rating=max_rating,
        ACCR=sum_ACCR,
        ARAP=sum_ARAP,
    )

def join_items(element):
    """
    Combine data from multiple datasets based on common keys.

    Args:
        element: An element representing a group of invoices.

    Yields:
        Invoice: A combined invoice.
    """
    tier = element[1][1][0]
    for item in element[1][0]:
        yield InvoiceItem(
            legal_entity=item.legal_entity,
            counter_party=item.counter_party,
            rating=item.rating,
            status=item.status,
            value=item.value,
            tier=tier,
        )

def main():
    with beam.Pipeline() as pipeline:
        logger.info("Initializing the data processing pipeline")
        logger.info("Reading and combining the input dataset files")

        # Read input datasets
        read_dataset1 = beam.io.ReadFromCsv("inputs/dataset1.csv", splittable=False)
        read_dataset1.label = "Read dataset1"
        dataset1 = pipeline | read_dataset1 | beam.Map(lambda x: (x.counter_party, x))
        
        read_dataset2 = beam.io.ReadFromCsv("inputs/dataset2.csv", splittable=False)
        read_dataset2.label = "Read dataset2"
        dataset2 = pipeline | read_dataset2
        
        # Join the dataset using ParDo and CoGroupByKey  
        joined_data = (
            [dataset1, dataset2]
            | "Group by counter_party and Merge datasets" >> beam.CoGroupByKey()
            | "Join datasets with common keys" >> beam.ParDo(join_items).with_output_types(InvoiceItem)
        )

        # Perform data aggregation and group data by different criteria(group_keys)
        legal_entity_group = (
            joined_data
            | "Group by legal_entity and Compute result_set1" >> beam.GroupBy("legal_entity")
            | "Compute and Process result_set1" >> beam.Map(process_invoice, 1).with_output_types(Result)
        )

        counter_legal_group = (
            joined_data
            | "Group by legal_entity and counter_party and Compute result_set2"
            >> beam.GroupBy("counter_party", "legal_entity")
            | "Compute and Process result_set2" >> beam.Map(process_invoice, 2).with_output_types(Result)
        )

        counter_party_group = (
            joined_data
            | "Group by counter_party and Compute result_set3" >> beam.GroupBy("counter_party")
            | "Compute and Process result_set3" >> beam.Map(process_invoice, 3).with_output_types(Result)
        )

        tier_group = (
            joined_data
            | "Group by tier and Compute result_set4" >> beam.GroupBy("tier")
            | "Compute and Process result_set4" >> beam.Map(process_invoice, 4).with_output_types(Result)
        )

        # Merge processed PCollections
        merged_pcollection = (
            legal_entity_group,
            counter_legal_group,
            counter_party_group,
            tier_group,
        ) | "Merge all Processed Data PCollections" >> beam.Flatten()

        # Write PCollection to a CSV file
        (merged_pcollection | "Write the Merged Data to a CSV file" >> beam.io.WriteToCsv(path="outputs/apache_output.csv"))

    # Log the completion of the data processing pipeline
    logger.info("Data processing pipeline completed. Output written to 'apache_output.csv'.")


if __name__ == "__main__":
    main()
