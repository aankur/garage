²
# Specification of the Garage K2V API (K2V = Key/Key/Value)

- We are storing triplets of the form `(partition key, sort key, value)` -> no
  user-defined fields, the client is responsible of writing whatever he wants
  in the value (typically an encrypted blob). Values are binary blobs, which
  are always represented as their base64 encoding in the JSON API. Partition
  keys and sort keys are utf8 strings.

- Triplets are stored in buckets; each bucket stores a separate set of triplets

- Bucket names and access keys are the same as for accessing the S3 API

- K2V triplets exist separately from S3 objects. K2V triples don't exist for
  the S3 API, and S3 objects don't exist for the K2V API.

- Values stored for triples have associated causality information, that enables
  Garage to detect concurrent writes. In case of concurrent writes, Garage
  keeps the concurrent values until a further write supersedes the concurrent
  values. This is the same method as Riak KV implements. The method used is
  based on DVVS (dotted version vector sets), described in the paper "Scalable
  and Accurate Causality Tracking for Eventually Consistent Data Stores", as
  well as [here](https://github.com/ricardobcl/Dotted-Version-Vectors)



## API Endpoints

### Operations on single items

**ReadItem: `GET /<bucket>/<partition key>?sort_key=<sort key>`**


Query parameters:

| name | default value | meaning |
| - | - | - |
| `sort_key` | **mandatory** | The sort key of the item to read |

Returns the item with specified partition key and sort key. Values can be
returned in either of two ways:

1. a JSON array of base64-encoded values, or `null`'s for tombstones, with
   header `Content-Type: application/json`

2. in the case where there are no concurrent values, the single present value
   can be returned directly as the response body (or an HTTP 204 NO CONTENT for
   a tombstone), with header `Content-Type: application/octet-stream`

The choice between return formats 1 and 2 is directed by the `Accept` HTTP header:

- if the `Accept` header is not present, format 1 is always used

- if `Accept` contains `application/json` but not `application/octet-stream`,
  format 1 is always used

- if `Accept` contains `application/octet-stream` but not `application/json`,
  format 2 is used when there is a single value, and an HTTP error 409 (HTTP
  409 CONFLICT) is returned in the case of multiple concurrent values
  (including concurrent tombstones)

- if `Accept` contains both, format 2 is used when there is a single value, and
  format 1 is used as a fallback in case of concurrent values

- if `Accept` contains none, HTTP 406 NOT ACCEPTABLE is raised

Example query:

```
GET /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
```

Example response:

```json
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/json

[
  "b64cryptoblob123",
  "b64cryptoblob'123"
]
```

Example response in case the item is a tombstone:

```
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken999
Content-Type: application/json

[
  null
]
```

Example query 2:

```
GET /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
Accept: application/octet-stream
```

Example response if multiple concurrent versions exist:

```
HTTP/1.1 409 CONFLICT
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream
```

Example response in case of single value:

```
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream

cryptoblob123
```

Example response in case of a single value that is a tombstone:

```
HTTP/1.1 204 NO CONTENT
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream
```

**InsertItem: `PUT /<bucket>/<partition key>?sort_key=<sort_key>`**

Inserts a single item. This request does not use JSON, the body is sent directly as a binary blob.

To supersede previous values, the HTTP header `X-Garage-Causality-Token` should
be set to the causality token returned by a previous read on this key. This
header can be ommitted for the first writes to the key.

Example query:

```
PUT /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
X-Garage-Causality-Token: opaquetoken123

myblobblahblahblah
```

Example response:

```
HTTP/1.1 200 OK
```

**DeleteItem: `DELETE /<bucket>/<partition key>?sort_key=<sort_key>`**

Deletes a single item. The HTTP header `X-Garage-Causality-Token` must be set
to the causality token returned by a previous read on this key, to indicate
which versions of the value should be deleted. The request will not process if
`X-Garage-Causality-Token` is not set.

Example query:

```
DELETE /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
X-Garage-Causality-Token: opaquetoken123
```

Example response:

```
HTTP/1.1 204 NO CONTENT
```

### Operations on index

**ReadIndex: `GET /<bucket>?start=<start>&end=<end>&limit=<limit>`**

Lists all partition keys in the bucket for which some triplets exist, and gives
for each the number of triplets (or an approximation thereof, this value is
	asynchronously updated, and thus eventually consistent).

Query parameters:

| name | default value | meaning |
| - | - | - |
| `start` | `null` | First partition key to list, in lexicographical order |
| `end` | `null` | Last partition key to list (excluded) |
| `limit` | `null` | Maximum number of partition keys to list |

The response consists in a JSON object that repeats the parameters of the query and gives the result (see below).

The listing starts at partition key `start`, or if not specified at the
smallest partition key that exists.  It returns partition keys in increasing
order and stops when either of the following conditions is met:

1. if `end` is specfied, the partition key `end` is reached or surpassed (if it
   is reached exactly, it is not included in the result)

2. if `limit` is specified, `limit` partition keys have been listed

3. no more partition keys are available to list

In case 2, and if there are more partition keys to list before condition 1
triggers, then in the result `more` is set to `true` and `nextStart` is set to
the first partition key that couldn't be listed due to the limit. In the first
case (if the listing stopped because of the `end` parameter), `more` is not set
and the `nextStart` key is not specified.

Example query:

```
GET /my_bucket HTTP/1.1
```

Example response:

```json
HTTP/1.1 200 OK

{ 
  start: null,
  end: null,
  limit: null,
  partition_keys: [
    [ "keys", 3043 ],
    [ "mailbox:INBOX", 42 ],
    [ "mailbox:Junk", 2991 ],
    [ "mailbox:Trash", 10 ],
    [ "mailboxes", 3 ],
  ],
  more: false,
  nextStart: null,
} 
```


### Operations on batches of items

**InsertBatch: `POST /<bucket>`**

Simple insertion and deletion of triplets. The body is just a list of items to
insert in the following format: `[ "<partition key>", "<sort key>", "<causality
token>"|null, "<value>"|null ]`. 

The causality token should be the one returned in a previous read request (e.g.
by ReadItem or ReadBatch), to indicate that this write takes into account the
values that were returned from these reads, and supersedes them causally. If
the triple is inserted for the first time, the causality token should be set to
`null`.

The value is expected to be a base64-encoded binary blob. The value `null` can
also be used to delete the triple while preserving causality information: this
allows to know if a delete has happenned concurrently with an insert, in which
case both are preserved and returned on reads (see below).

Partition keys and sort keys are utf8 strings which are stored sorted by
lexicographical ordering of their binary representation.

Example query:

```json
POST /my_bucket HTTP/1.1

[
  [ "mailbox:INBOX", "001892831", "opaquetoken321", "b64cryptoblob321updated" ],
  [ "mailbox:INBOX", "001892912", null, "b64cryptoblob444" ],
  [ "mailbox:INBOX", "001892932", "opaquetoken654", null ],
]
```

Example response:

```
HTTP/1.1 200 OK
```


**ReadBatch: `POST /<bucket>?search`**, or alternatively<br/>
**ReadBatch: `SEARCH /<bucket>`**

Batch read of triplets in a bucket.

The request body is a JSON list of searches, that each specify a range of
items to get (to get single items, set `single_item` to `true`). A search is a
JSON struct with the following fields:

| name | default value | meaning |
| - | - | - |
| `partition_key` | **mandatory** | The partition key in which to search |
| `start` | `null` | The sort key of the first item to read |
| `end` | `null` | The sort key of the last item to read (excluded) |
| `limit` | `null` | The maximum number of items to return |
| `single_item` | `false` | Whether to return only the item with sort key `start` |
| `conflicts_only` | `false` | Whether to return only items that have several concurrent values |
| `tombstones` | `false` | Whether or not to return tombstone lines to indicate the presence of old deleted items |

 
For each of the searches, triplets are listed and returned separately. The
semantics of `start`, `end` and `limit` is the same as for ReadIndex. The
additionnal parameter `single_item` allows to get a single item, whose sort key
is the one given in `start`. Parameters `conflicts_only` and `tombstones`
control additional filters on the items that are returned.

The result is a list of length the number of searches, that consists in for
each search a JSON object specified similarly to the result of ReadIndex, but
that lists triples within a partition key.

The format of returned tuples is as follows: `[ "<sort key>", "<causality
token>", "<value1>", ...]`, with the following fields:

- sort key: any unicode string used as a sort key

- causality token: an opaque token served by the server (generally
  base64-encoded) to be used in subsequent writes to this key

- value: binary blob, always base64-encoded

- if several concurrent values exist, they are appended at the end

- in case of concurrent update and deletion, a `null` is added to the list of concurrent values

- if the `tombstones` query parameter is set to `true`, tombstones are returned
  for items that have been deleted (this can be usefull for inserting after an
  item that has been deleted, so that the insert is not considered
  concurrent with the delete). Tombstones are returned as tuples in the
  same format with only `null` values

Example query:

```json
POST /my_bucket?search HTTP/1.1

[
  {
    partition_key: "mailboxes",
  },
  {
    partition_key: "mailbox:INBOX",
    start: "001892831",
    limit: 3,
  },
  {
    partition_key: "keys",
    start: "0",
    single_item: true,
  },
]
```

Example associated response body:

```json
HTTP/1.1 200 OK

[
  {
    partition_key: "mailboxes",
    start: null,
    end: null,
    limit: null,
    conflicts_only: false,
    tombstones: false,
    single_item: false,
    items: [
      [ "INBOX", "opaquetoken123", "b64cryptoblob123", "b64cryptoblob'123" ],
      [ "Trash", "opaquetoken456", "b64cryptoblob456" ],
      [ "Junk", "opaquetoken789", "b64cryptoblob789" ],
    ],
    more: false,
    nextStart: null,
  },
  {
    partition_key: "mailbox::INBOX",
    start: "001892831",
    end: null,
    limit: 3,
    conflicts_only: false,
    tombstones: false,
    single_item: false,
    items: [
      [ "001892831", "opaquetoken321", "b64cryptoblob321" ],
      [ "001892832", "opaquetoken654", "b64cryptoblob654" ],
      [ "001892874", "opaquetoken987", "b64cryptoblob987" ], 
    ],
    more: true,
    nextStart: "001892898",
  },
  {
    partition_key: "keys",
    start: "0",
    end: null,
    conflicts_only: false,
    tombstones: false,
    limit: null,
    single_item: true,
    items: [
      [ "0", "opaquetoken999", "b64binarystuff999" ],
    ],
    more: false,
    nextStart: null,
  },
]
```



**DeleteBatch: `POST /<bucket>?delete`**

Batch deletion of triplets. The request format is the same for `POST
/<bucket>?search` to indicate items or range of items, except that here they
are deleted instead of returned, but only the fields `partition_key`, `start`,
`end`, and `single_item` are supported. Causality information is not given by
the user: this request will internally list all triplets and write deletion
markers that supersede all of the versions that have been read.

This request returns for each series of items to be deleted, the number of
matching items that have been found and deleted.

Example query:

```json
POST /my_bucket?delete HTTP/1.1

[
  {
    partition_key: "mailbox:OldMailbox",
  },
  {
    partition_key: "mailbox:INBOX",
    start: "0018928321",
    single_item: true,
  },
]
```

Example response:

```
HTTP/1.1 200 OK

[
  {
    partition_key: "mailbox:OldMailbox",
    start: null,
    end: null,
    single_item: false,
    deleted_items: 35,    
  },
  {
    partition_key: "mailbox:INBOX",
    start: "0018928321",
    end: null,
    single_item: true,
    deleted_items: 1,
  },
]
```


## Internals: causality tokens

The method used is based on DVVS (dotted version vector sets). See:

- the paper "Scalable and Accurate Causality Tracking for Eventually Consistent Data Stores"
- <https://github.com/ricardobcl/Dotted-Version-Vectors>

For DVVS to work, write operations (at each node) must take a lock on the data table.
