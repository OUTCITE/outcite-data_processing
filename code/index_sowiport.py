import sys, os
import json
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
from copy import deepcopy as copy


_dumpfolder = sys.argv[1];#'crossref-works.2018-01-21.json.xz';
_index      = 'sowiport'
_chunk_size = 1000;

_body = {
    '_op_type': 'index',
    '_index':   _index,
    '_id':      None,
    '_source':  None
}


def remodel(doc):
    return doc;

def load_sowiport(filename):
    IN = open(filename);
    for line in IN.readlines():
        line_           = line.rstrip();
        line_           = line_[:-1] if line_.endswith(',') else line_;
        body            = copy(_body);
        doc             = json.loads(line_);
        body['_id']     = doc['_id'];
        body['_source'] = remodel(doc['_source']);
        yield body;
    IN.close();

def load_folder(foldername):
    for filename in os.listdir(foldername):
        for body in load_sowiport(foldername+filename):
            yield body;

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
