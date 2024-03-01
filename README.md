# s3db
S3 as a simple KV json store using mongo inspired syntax.

# Usage
```
## Connection
db = S3db('example-s3db-test') # S3 bucket
collection = db.users # prefix

## Insert record
collection.insert('pcox', {'first_name': 'Patrick',
                           'last_name': 'Cox',
                           'age': 20,
                           'bio': 'M',
                           'location': 'Arlington, TX'})

## Fetch record
record = collection.select('pcox')
print(record)

## Fetch all records
records = collection.fetchall()
print(records)

## Number of records
count = collection.count()
print(count)

# Find record that contains value(s) (this is expensive and slow)
records = collection.find({'age': 20})
print(records)
records = collection.find({'bio': 'M', 'location': 'Arlington, TX'})
print(records)

# Delete records
collection.delete('pcox')
```


