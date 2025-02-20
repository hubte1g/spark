from pyspark.sql.types import StructType

def compare_schemas(schema1: StructType, schema2: StructType):
    fields1 = {f.name: f.dataType for f in schema1.fields}
    fields2 = {f.name: f.dataType for f in schema2.fields}
    all_fields = set(fields1.keys()) | set(fields2.keys())
    
    differences = []
    for field in all_fields:
        if field not in fields1:
            differences.append((field, "Missing in Schema1", fields2[field]))
        elif field not in fields2:
            differences.append((field, fields1[field], "Missing in Schema2"))
        elif fields1[field] != fields2[field]:
            differences.append((field, fields1[field], fields2[field]))
    
    return differences
