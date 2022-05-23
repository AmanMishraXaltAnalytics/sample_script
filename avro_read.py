import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json

# AVRO FILE READ
reader = DataFileReader(open(r"C:\Users\u27f91\Downloads\part-00000-949a23f3-ef9b-438b-9fa9-53a5be41e6e3-c000.avro", "rb"), DatumReader())
for user in reader:
    print(user)

# AVRO SCHEMA
reader= avro.datafile.DataFileReader(open(r"C:\Users\u27f91\Downloads\part-00000-ca3d6282-e6e7-4a15-a04a-0e808e1b2468-c000.avro","rb"),avro.io.DatumReader())
schema=reader.meta
print(schema)

## PARSING AVRO SCHEMA
# schema =avro.schema.parse(open("avro_1.avsc","rb").read())
# writer= DataFileWriter(open("avro.avro","wb"), DatumWriter(),schema)
# with open("json.json") as fp:
#     contents=json.load(fp)
# # print(contents)
# writer.append(contents)
# print(writer) 
 