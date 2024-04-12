from fastavro import writer, parse_schema


schema = {
    'type': 'record',
    'namespace': 'job_2',
    'name': 'sales',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'}
    ]
}

parsed_schema = parse_schema(schema)


def json_to_avro(json_data, file_name):
    """
    Converts a list of dictionaries (JSON data) into an Avro file using a predefined Avro schema.
    """
    with open(file_name, 'wb') as file:
        writer(file, parsed_schema, json_data)
