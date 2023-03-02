import sys, os
import json
import gzip
import time
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
from copy import deepcopy as copy


_collection = sys.argv[1]; #'gesis_bib' OR 'research_data'
_upsert     = True if len(sys.argv) > 2 and sys.argv[2]=='upsert' else False;
_index      = _collection;
_batch_size =  500;
_chunk_size = 1000;

_max_scroll_tries = 2;
_max_extract_time = 0.5;

_body = {
    '_op_type': 'update' if _upsert else 'index',
    '_index':   _index,
    '_id':      None,
    '_source':  { 'doc':None, 'doc_as_upsert':True } if _upsert else None
}


def remodel(source,collection):
    #TODO: Changes necessary?
    return source;

def get_items_gws(batch,maxlen,index_source,typ):
    client    = ES(['search.gesis.org/'],scheme='http',port=9200,timeout=60); #{"bool": {"must":[{"term":{"index_source": index_source }},{"term":{"type":typ}}]}}
    query     = {"bool": {"must":[{"match":{"index_source":index_source}},{"term":{"type":typ}}]}} if index_source!=None else {"term":{"type":typ}};
    #query     = {"match":{"index_source":index_source}} if index_source!=None else {"term":{"type":typ}};
    page      = client.search(index='gesis',scroll='2m',size=batch,query=query);
    sid       = page['_scroll_id'];
    returned  = len(page['hits']['hits']);print(query,returned)
    page_num  = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            yield doc['_id'],doc['_source'];
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*batch))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);

def load_gws(collection):
    for ID,source in get_items_gws(_batch_size,999999999999999999999,[None,'GESIS-DBK'][_collection=='research_data'],collection):
        body = copy(_body);
        body['_id'] = ID;
        if _upsert:
            body['_source']['doc'] = remodel(source,collection);
        else:
            body['_source'] = remodel(source,collection);
        yield body;


_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,load_gws(_collection),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % _chunk_size == 0:
        print('###############################',i,'###');
        _client.indices.refresh(index=_index);
_client.indices.refresh(index=_index);
#'''
