import sys
import json
import lzma
import gzip
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
from copy import deepcopy as copy
import codecs



_dumpfile   = sys.argv[1];#'crossref-works.2018-01-21.json.xz';
_index      = 'crossref'
_chunk_size = 10000;

_compressor = lzma;

_body = {
    '_op_type': 'index',
    '_index':   _index,
    '_id':      None,
    '_source':  None
}


# object mapping for [license.start.timestamp] tried to parse field [timestamp] as object, but found a concrete value

def remodel(doc):
    del doc['_id'];
    if 'created' in doc:
        if 'timestamp' in doc['created']:
            if isinstance(doc['created']['timestamp'],dict) and '$numberLong' in doc['created']['timestamp']:
                doc['creation_time'] = doc['created']['timestamp']['$numberLong'];
        del doc['created'];
    if 'deposited' in doc:
        if 'timestamp' in doc['deposited']:
            if isinstance(doc['deposited']['timestamp'],dict) and '$numberLong' in doc['deposited']['timestamp']:
                doc['deposit_time'] = doc['deposited']['timestamp']['$numberLong'];
        del doc['deposited'];
    if 'indexed' in doc:
        if 'timestamp' in doc['indexed']:
            if isinstance(doc['indexed']['timestamp'],dict) and '$numberLong' in doc['indexed']['timestamp']:
                doc['index_time'] = doc['indexed']['timestamp']['$numberLong'];
        del doc['indexed'];
    if 'update-to' in doc:
        if 'updated' in doc['update-to']:
            if 'timestamp' in doc['update-to']['updated']:
                if isinstance(doc['update-to']['updated']['timestamp'],dict) and '$numberLong' in doc['update-to']['updated']['timestamp']:
                    doc['update_time'] = doc['update-to']['updated']['timestamp']['$numberLong'];
        del doc['update-to'];
    if 'license' in doc:
    #    for i in range(len(doc['license'])):
    #        if isinstance(doc['license'][i],dict) and 'start' in doc['license'][i]:
    #            if isinstance(doc['license'][i]['start'],dict) and 'timestamp' in doc['license'][i]['start']:
    #                if isinstance(doc['license'][i]['start']['timestamp'],dict) and '$numberLong' in doc['license'][i]['start']['timestamp']:
    #                    doc['license'][i]['start']['timestamp'] = doc['license'][i]['start']['timestamp']['$numberLong'];
        del doc['license'];
    return doc;

def check(doc):
    try:
        doc['created']['timestamp']              = doc['created']['timestamp']['$numberLong'];
    except:
        pass;
    try:
        doc['deposited']['timestamp']            = doc['deposited']['timestamp']['$numberLong'];
    except:
        pass;
    try:
        doc['indexed']['timestamp']              = doc['indexed']['timestamp']['$numberLong'];
    except:
        pass;
    try:
        doc['update-to']['updated']['timestamp'] = doc['update-to']['updated']['timestamp']['$numberLong'];
    except:
        pass;
    try:
        for i in range(len(doc['license'])):
            doc['license'][i]['start']['timestamp']  = doc['license'][i]['start']['timestamp']['$numberLong'];
    except:
        pass;
    return doc;

def load_crossref_(filename):
    #reader = codecs.getreader("utf-8")
    IN = _compressor.open(filename, mode='rt');
    for line in IN:
        body            = copy(_body);
        doc             = json.loads(line);
        body['_id']     = doc['_id']['$oid'];
        body['_source'] = check(remodel(doc));
        yield body;
    IN.close();

def make_id(doi):
    string = str(hash(doi));
    string = '1'+string[1:] if string[0] == '-' else '0'+string;
    return string;

def load_crossref(filename):
    IN   = open(filename);#_compressor.open(filename, mode='rt');
    JSON = json.load(IN);    
    for doc in JSON['items']:
        body            = copy(_body);
        body['_id']     = make_id(doc['DOI']);
        body['_source'] = remodel(doc);
        yield body;
    IN.close();


_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,load_crossref_(_dumpfile),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % _chunk_size == 0:
        print('###############################',i,'###');
        _client.indices.refresh(index=_index);

