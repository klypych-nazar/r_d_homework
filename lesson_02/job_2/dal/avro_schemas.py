from fastavro import parse_schema


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
