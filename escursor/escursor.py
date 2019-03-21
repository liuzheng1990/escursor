from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def get_total_hits(es_conn, index, doc_type, body):
	body = body.copy()
	if 'size' not in body: 
		body['size'] = 0
	res = es_conn.search(index=index, doc_type=doc_type, body=body)
	return res['hits']['total']

class ESCursor:
	
	# class constant, can be used directly as ESCursor.default_query_body.
	default_query_body = {'query': {"match_all": {}}}

	def __init__(self, es_conn, index, doc_type="_doc", body=default_query_body, 
											total_number=None, batch_size=10000):
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


def get_id_iter(es, index, doc_type): # get the _id of each document
	query_body = {'query': {'match_all': {}}, "stored_fields": []}
	it = scan(es, query=query_body, index=index, doc_type=doc_type)
	for rec in it:
		yield rec['_id']



if __name__ == "__main__":

	es = Elasticsearch()
	body = ESCursor.default_query_body
	body['stored_fields'] = []
	es_cursor = ESCursor(es, "bank", "contact", body=body, total_number=200, batch_size=10)

	for id in es_cursor:
		print(id)

