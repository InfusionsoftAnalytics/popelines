type_conversion = {
    'INTEGER': 'INT64',
    'STRING': 'STRING',
    'RECORD': 'STRUCT',
    'TIMESTAMP': 'TIMESTAMP',
    'BOOLEAN': 'BOOLEAN',
    'FLOAT': 'FLOAT64',
    'DATE': 'DATE'
}

def process_struct_type(field, spaces, key_callback):
    if field.mode == "REPEATED":
        type_q = f"\n{spaces * ' '}ARRAY<STRUCT<\n"
    else:
        type_q = f"\n{spaces * ' '}STRUCT<\n"
    
    subfields = field.fields
    for subfield in subfields:
        if subfield.field_type != 'RECORD':
            if subfield.mode == "REPEATED":
                type_q += f"{(spaces + 2 )* ' '}`{key_callback(subfield.name)}` ARRAY<{type_conversion[subfield.field_type]}>,\n"
            else:
                type_q += f"{(spaces + 2 )* ' '}`{key_callback(subfield.name)}` {type_conversion[subfield.field_type]},\n"
        else:
            type_q += f"{(spaces + 2) * ' '}`{key_callback(subfield.name)}` {process_struct_type(subfield, spaces + 2, key_callback)},\n"
            
    type_q = type_q.rstrip(',\n').rstrip(',')
    
    if field.mode == "REPEATED":
        type_q += f"\n{spaces * ' '}>>"
    else:
        type_q += f"\n{spaces * ' '}>"
    
    return type_q
    
def process_struct_data(field, parent_cols, spaces):
    if field.mode == "REPEATED":
        data_q = f"\n{spaces * ' '}[STRUCT(\n"
        parent_cols = [field.name]
    else:
        data_q = f"\n{spaces * ' '}STRUCT(\n"
    
    subfields = field.fields
    for subfield in subfields:
        if subfield.field_type != 'RECORD':
            data_q += f"{(spaces + 2) * ' '}{'.'.join([f'`{x}`' for x in parent_cols + [subfield.name]]).strip(',')},\n"
        else:
            data_q += f"{(spaces + 2) * ' '}{process_struct_data(subfield, parent_cols + [subfield.name], spaces + 2)}"
            
    data_q = data_q.strip(',\n')
            
    if field.mode == "REPEATED":
        data_q += ")],\n"
    else:
        data_q += "),\n"
            
    return data_q

def process_cross_joins(field, parent_table):
    cross_joins = []
    
    # repeated fields of record type must be added
    if field.mode == "REPEATED" and field.field_type == "RECORD":
        cj = f'\nLEFT JOIN UNNEST({parent_table}.{field.name}) {field.name}'
        cross_joins.append(cj)
        
    # if we come across a field that is a record, but is not repeated, then some
    # of its subfields may be repeated, so we need to recurse through it
    if field.field_type == 'RECORD':
        if field.mode == 'REPEATED':
            parent_table = field.name
        else:
            parent_table = parent_table + '.' + field.name
        
        for subfield in field.fields:
            cjs = process_cross_joins(subfield, parent_table)

            for cj in cjs:
                if cj not in cross_joins:
                    cross_joins.append(cj)
    
    return cross_joins

def process_field(field, prefix, key_callback):
    field_text = ''
    if field.field_type != 'RECORD':
        field_name = key_callback(field.name)
        if field_name:
            field_text = f"  `copy_table`.`{field_name}`,\n"
    else:
        type_q = process_struct_type(field, 2, key_callback)
        data_q = process_struct_data(field, [field.name], 2).lstrip().lstrip("\nSTRUCT")
        field_text = type_q.strip(",\n") + data_q
        field_text = field_text.strip(",\n")
        field_text += f" `{field.name.lower()}`,\n"
        
    return field_text