import sys, os
import json
import gzip
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
from copy import deepcopy as copy
import dateparser


_dumpfolder = sys.argv[1];
_index      = 'openalex'
_chunk_size = 1000;

_compressor = gzip;#lzma;

_body = {
    '_op_type': 'index',
    '_index':   _index,
    '_id':      None,
    '_source':  None
}


# object mapping for [license.start.timestamp] tried to parse field [timestamp] as object, but found a concrete value

def remodel(doc):
    
    return doc;

def load_openalex(filename):
    IN   = _compressor.open(filename, mode='rt');
    for line in IN:
        body            = copy(_body);
        try:
            doc = json.loads(line);
        except Exception as e:
            print(e); print('Some problem with this line:',line);
            continue;
        body['_id']     = doc['id'];
        body['_source'] = remodel(doc);
        yield body;
    IN.close();

def load_folder(foldername):
    for filename in os.listdir(foldername):
        for body in load_openalex(foldername+filename):
            yield body;

#for body in load_folder(_dumpfolder):
#    print(body);

#'''
_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,load_folder(_dumpfolder),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % _chunk_size == 0:
        print('###############################',i,'###');
        _client.indices.refresh(index=_index);
#'''
