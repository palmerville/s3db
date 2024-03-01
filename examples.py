#!/usr/bin/env python3

from s3db import S3db

# Connection
db = S3db('example-s3db-test') # S3 bucket
collection = db.users # prefix


# Insert records
collection.insert('pcox', {'first_name': 'Patrick',
                           'last_name': 'Cox',
                           'age': 20,
                           'bio': 'M',
                           'location': 'Arlington, TX'})

collection.insert('kgreen', {'first_name': 'Kevin',
                             'last_name': 'Green',
                             'age': 37,
                             'bio': 'M',
                             'location': 'Arlington, TX'})

collection.insert('jmorris', {'first_name': 'Jeremy',
                              'last_name': 'Morris',
                              'age': 36,
                              'bio': 'M',
                              'location': 'Lawrence, KS'})

collection.insert('mward', {'first_name': 'Mary',
                            'last_name': 'Ward',
                            'age': 25,
                            'bio': 'F',
                            'location': 'Lawrence, KS'})


# Fetch record
record = collection.select('pcox')
print(record)


# Fetch all records
records = collection.fetchall()
print(records)


# Number of records
count = collection.count()
print(count)


# Find record that contains value(s) (this is expensive and slow)
records = collection.find({'age': 25})
print(records)
records = collection.find({'bio': 'M', 'location': 'Arlington, TX'})
print(records)


# Delete records
collection.delete('pcox')
collection.delete('kgreen')
collection.delete('jmorris')
collection.delete('mward')


