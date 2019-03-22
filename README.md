# How to DIY a cursor class for Elasticsearch (and why it doesn't work well)

This repo is created for my [blog article](https://liutheprogrammer.wordpress.com/2019/03/21/how-to-diy-a-cursor-class-for-elasticsearch/). I'm also posting that article here.

New to Elasticsearch, I have spend a significant amount of time to figure out how to iterate through a large data set. In the beginning, it looked strange to me that **Elasticsearch does not return a cursor when one makes a query**. Instead, it only returns a page of the full data set (with page size default to 10). To a person more familiar with python's SQL and MongoDB drivers and less with Elasticsearch's driver, I felt at loss when a query did not return me a cursor. Although I heard that the Elasticsearch helpers might be designed for this purpose, I didn't came to it directly. Instead, my attention was attracted by the **"from" and "size" parameters in the query body**.

"Hmm. It seems I can retrieve the query results in batches. With some knowledge of Python's **iterator class protocol**, I should be able to wrap the returned data into a cursor myself!"

And I wrote one. Although in the end it didn't work well for large data sets, I think it's worth writing it down. It will benefit readers who:

1. want to see a real-world example of Python's iterator class protocol,
2. want to know the limitation of Elasticsearch's "from" and "size" parameters, or
3. simply want to know the right way to scan iterate through query results (jump to the last section for the scroll API, and in particular, the last code block for Python's `elasticsearch.helpers.scan` function).

I will first give a brief introduction on how to make a Python iterator class. Then we will go ahead and write the `ESCursor` class and give it some test. Then I'll show it crashes on a large dataset. In the end, I'll discuss the scroll API and the Python helper called `scan`.

Let's get started.

<!--more-->

## Python's iterator class

How can we make our own class object an iterator? If the class support iteration, one should be able to write the following code

```python
foo = Foo() # a class supporting iteration.
for x in foo:
	process(x) # process x
```

Indeed, the cursors given by a SQL query or a MongoDB query is nothing but such an object.

A good short discussion about the iterator class interface can be found [here](https://www.w3schools.com/python/python_iterators.asp). To be brief, any Python class which has the methods `__iter__` and `__next__` implemented is an iterator class. The first method is used to initialize the iterator, and the second to retrieve the next item. If the iteration should terminate at some point, the `__next__` method should throw a `StopIteration` exception. Therefore, what a for loop does is basically calling the `__iter__` to initialize it, then calling the `__next__` function to obtain the next item. When it catches the `StopIteration` exception, it breaks the loop.

In other words, the following two pieces of codes are equivalent.

for - loop version
```python
foo = Foo()
for x in foo:
	process(x) # do something about x
```

generic version using while loop

```python
foo = Foo()
iter(foo) # equivalent to foo.__iter__()
try:
	while True:
		x = next(foo) # alternatively, use x = foo.__next__()
		process(x)
except StopIteration:
	pass
```

This not only demystifies how to over iterators, but also gives some hints on how the `__next__` and `__iter__` function should be implemented. The `__iter__` function do whatever needed for initialization, and (not demonstrated in the above codes) return the iterator object (usually `self`). The `__next__` method keep tract of the current state, and return the next item.

## Get a batch in Elasticsearch query

A few wrods to readers new to Elasticsearch. Elasticsearch is a very excellent database supporting searching in various ways. You can check [here](https://tryolabs.com/blog/2015/02/17/python-elasticsearch-first-steps/) for a simple but interesting (especially to Star War fans) tutorial.

Elasticsearch provides a standard RESTful API interface for communicating with the database server. For example, to match all documents in an index (imagine an "index" as "database") and a doc_type (imagine it as a "table" in SQL jargon, or a "collection" in MongoDB jargon), we simply send a POST request to the url `http://localhost:9200///_search`. The request body describes the query. In this "match all" scenario, it looks like the following:

```
# written in simplified HTTP style
POST /<index>/<doc_type>/_search

{
	"query": {"match_all": {}}
}
```

If you have a large dataset, you'll immediately notice something strange. Look at the "hits" field in the response data. It's a dictionary of the following format:

```
{
	"total: <total number of documents matched>,
	"max_score": <maximum matching score>,
	"hits": [{...}, ...] # list of all matched documents
}
```

Since I have a big database, the `total` field give more than 20 million (since I ran a "match all" query, that basically means that there were 20 million documents in my database). However, the interior `hits` field, which is supposed to contain all documents matched, does not contain 20 million documents (it would be a nightmare if it did). Instead, only 10 documents were included.

Our task now is to figure out how to retrieve all data iteratively from the Elasticsearch server.

## Turn pages with "from" and "size"

The upsort in the last section is that instead of returning all matched documents in one query, Elasticsearch divide them into pages. As you can guess now, the default page size is 10 documents. We can easily specify which page to get and the size of each page by using "from" and "size" in the query body. See we want to keep the default 10-doc-per-page size, but we want the data on the 9th page, we do the following.

```
POST /<index>/<doc_type>/_search

{
	"query": {"match_all": {}},
	"size": 10,
	"from": 81 # note the first page starts from 0 to 10.
}
```

With the help of `size` and `from`, we should be able to gradually take the documents on each page from cover to cover. If we iterate over documents, all we have to do is to **take a batch, enumerate each document in that batch, and then take another batch, until we exhaust all batches**.

## ESCursor class

Now let's go ahead and construct this cursor class.

The module can be found in [my Github repo](https://github.com/liuzheng1990/escursor). The main class is the `ESCursor` class as follows.

```python
class ESCursor:
	
	# class constant, can be used directly as ESCursor.default_query_body.
	default_query_body = {'query': {"match_all": {}}}

	def __init__(self, es_conn, index, doc_type="_doc", body=default_query_body, 
							total_number=None, batch_size=1000):
		self.es_conn = es_conn
		self.index = index
		self.doc_type = doc_type
		self.body = body
		# total_number is the maximum number of docs user wants. 
		# If it's larger than the actual total number, 
		# use the actual total number instead.
		actual_total_number = get_total_hits(es_conn, index, doc_type, body)
		if total_number is None or total_number > actual_total_number:
			total_number = actual_total_number 
		self.total_number = total_number
		if batch_size < total_number:
			self.batch_size = batch_size # parameter to "size", when fetching each batch
		else:
			self.batch_size = total_number
		self.offset = 0 # the parameter to "from" keyword, when fetching each batch
		self.inbtach_idx = 0 # index in the current batch
		self.current_batch_size = 0 # the actual batch size. 
					    # May be smaller than "batch_size".

	def __iter__(self):
		"""
		function to initialize the iteration. 
		Set all parameters to its initial state, and fetch the first batch.
		"""
		self.offset = 0
		self.inbtach_idx = 0
		self.current_batch_size = 0
		self._fetch_next_batch()
		return self

	def _fetch_next_batch(self):
		"""
		fetch the next page.
		
		increment the offset, retrieve the data on the next page,
		compute the batch size, and move the inbatch_idx to the beginning
		of the batch.
		"""
		self.offset += self.current_batch_size
		if self.offset >= self.total_number:
			raise StopIteration
		body_batch = {**self.body, "from": self.offset, "size": self.batch_size}
		res_data = self.es_conn.search(index=self.index, 
					       doc_type=self.doc_type, 
					       body=body_batch)
		self.hits = res_data["hits"]["hits"]
		self.current_batch_size = len(self.hits)
		self.inbtach_idx = 0

	def __next__(self):
		"""
		Obtain the next document. If the current batch has
		been exhausted, it calls the "_fetch_next_batch"
		function to retrieve documents from the next apge.
		
		If all pages have been covered or if it goes beyond
		the limit set by the "total_number" parameter (see
		the constructor", then raise the "StopIteration"
		exception to end the iteration.
		"""
		if self.inbtach_idx >= self.current_batch_size:
			self._fetch_next_batch()
			if self.inbtach_idx == self.current_batch_size:
				raise StopIteration
		r = self.hits[self.inbtach_idx]
		self.inbtach_idx += 1
		return r


```
The comments in the codes are quite descriptive, hopefully. The "private" method `_fetch_next_batch` takes data in the next page, by adding `{'from': self.offset, 'size':self.batch_size}` to the query body.

Now the `ESCursor` object will really behave like a cursor. Let's say we want to retrive the "_id" fields of the first 200 documents. We can simply do the following.

```python
es = Elasticsearch() # elasticsearch connection to localhost:9200
body = ESCursor.default_query_body # see the class definition above
body['stored_fields'] = [] # add a field to prevent fields from returning
			   # we just need _id, and don't want to wait network resource
es_cursor = ESCursor(es, "bank", "customer", body=body, total_number=200, batch_size=10)
# here index="bank", doc_type="customer"

# iterating over the cursor
for id in es_cursor:
	print(id)
```

This closely immitates the cusor interface of SQL or pymongo. Note that in the constructor, I set `batch_size=10`. If you leave this parameter unspecified, it defaults to 1000. This number determines how many documents are on one page, but is not relevant to the users' viewpoint. It does affect the performance a bit, as a small `batch_size` means a frequent communication to the server, increasing the networking latency, while too big a `batch_size` means the server has to return a huge chunk of data within one http query. Here `batch_size=10` is only set for pedagogical reason.

## Iterating over a large dataset

With the joy to sea the above codes working, I delightfully applied it to my database containing 20 million documents. I guess you can already smell the smoke of it crashing and burning.

The error message is as follows.

>    raise HTTP_EXCEPTIONS.get(status_code, TransportError)(status_code, error_message, additional_info)
> elasticsearch.exceptions.TransportError: TransportError(500, 'search_phase_execution_exception', 'Result window is too large, from + size must be less than or equal to: [10000] but was [11000]. See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting.')

It mentioned two important things.

1. from + size must be less than or equal to [index.max_result_window] default to 10000.
2. The **scroll API** provides a more efficient way to request large data sets.

One can check the [official documents](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html) for some detailed discussion on the scroll API. The upshot is, when one does a query which potentially returns a large data set, he/she should add a url parameter called `scroll` and specify a lifespan. For example, in our "match all" query, we should write:

```
POST /<index>/<doc_type>/_search?scroll=5m

{
	"query": {"match_all": {}},
	"size": 100 # we don't specify "from" now
}
```
Here we created a **server-side scrolling cursor** which is to be kept alive for 5 minutes. The response to this query will contain a `_scroll_id` field. One can then go the the `/_search/scroll` endpoint to retrieve data back.

```POST /_search/scroll
{
	"scroll": "5m", #prolong cursor's life by another 5 mins
	"scroll_id": <_scroll_id given above>
}
```

Not surprisingly, Python's `elasticsearch` module has prepared a cursor-like generator function called `scan`. Here is the shortest answer to the question in the beginning.

```python
from elasticsearch.helpers import scan
it = scan(es, query=query_body, index=index, doc_type=doc_type)
for doc in it:
	process(doc)
```

To retrieve "_id" only, we can wrap up this iterator as follows.

```python
def get_id_iter(es, index, doc_type, query_body): # get the _id of each document
	query_body["stored_field"] = []
	it = scan(es, query=query_body, index=index, doc_type=doc_type)
	for rec in it:
		yield rec['_id']
```

Conclusion: In this article, we wrote a DIY cursor for the Elasticsearch python driver, using `size` and `from` in the query body. While it doesn't work well for large datasets, we demonstrated, using this example, how the database cursor objects are made of in Python, and discussed how the python iterator class works. It's always fun to build something from the beginning, even it doesn't work as well as its "official" counter-part.

Happy coding!
