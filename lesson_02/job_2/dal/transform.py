from fastavro import writer

from lesson_02.job_2.dal.avro_schemas import parsed_schema


def json_to_avro(json_data, file_name):
    """
    Converts a list of dictionaries (JSON data) into an Avro file using a predefined Avro schema.
    """
    with open(file_name, 'wb') as file:
        writer(file, parsed_schema, json_data)
