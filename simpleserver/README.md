**Description**

It's example of slowpoke usage

```
// Package contains sample handlers for working with boltdb/slowpoke.
// Handler server/database/store/
// database - bolt or slowpoke
// store - bucket for bolt, file for slowpoke
// designed for storing simple strings in keys


Examples

PUT:

# params
host/database/store/key
and value in body

curl -X PUT -H "Content-Type: application/octet-stream" --data-binary "@durov.jpg" localhost:5000/bolt/images/durov
curl -X PUT -H "Content-Type: text/html" -d '{"username":"xyz","password":"xyz"}' localhost:5000/bolt/users/user1
curl -X PUT -H "Content-Type: text/html" -d 'some value' localhost:5000/bolt/users/user2

GET:

# params
host/database/backet/key

curl localhost:5000/bolt/images/durov
return: bytes
curl localhost:5000/bolt/users/user1
return: {"username":"xyz","password":"xyz"}
curl -v localhost:5000/bolt/images/durov2
return 404 Error

POST:

# params
host/database/backet/key?cnt=1000&order=desc&vals=false

key: first key, possible values "some_your_key" or "some_your_key*" for prefix scan, Last, First - default Last
cnt: return count records, default 1000
order: sorting order (keys ordered as strings!), default desc
vals: return values, default false

curl -X POST localhost:5000/bolt/users
return: {"user2","user1"}

curl -X POST localhost:5000/bolt/users/
return: {"user1"}

curl -X POST "http://localhost:5000/bolt/users/use*?order=asc&vals=true"
return: {"user1":"{"username":"xyz","password":"xyz"}","user2":"some value"}

curl -X POST "http://localhost:5000/bolt/users/user2?order=desc&vals=true"
return: {"user2":"some value","user1":"{"username":"xyz","password":"xyz"}"}

DELETE:

curl -X DELETE http://localhost:5000/bolt/users/user2
return 200 Ok (or 404 Error if bucket! not found)
```