# gokvstore


## What is it?

Toy implementation of a key-value store in Go. It's a learning exercise for me to get more familiar file IO.

## How to use it?

New(dbName, walName) will instantiate a new KVStore. dbName is the name of the database file, 
and walName is the name of the write-ahead log file. The database file is a binary file that 
stores the key-value pairs. The write-ahead log file is a binary file that stores the operations
that have been performed on the database. The database file and the write-ahead log file are both
created if they don't exist.

If they do exist, the data from the files are loaded into memory. The write-ahead log file is 
then replayed.

These are the operations available on a KVStore:
 - Flush() - flushes the write-ahead log file to disk.
 - Coalesce() - coalesces the write-ahead log file and stores the data in the database file.
 - Get(key) - returns the value associated with the key
 - Set(key, value) - sets the value associated with the key
 - Delete(key) - deletes the key-value pair associated with the key
 - Close() - closes the database file and the write-ahead log file

