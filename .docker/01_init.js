db = db.getSiblingDB('test'); // Use 'test' database
db.createCollection('concurrency_poc');
print("Collection 'concurrency_poc' created.");