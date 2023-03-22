import sys
import json
import lzma
import gzip
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
from copy import deepcopy as copy


_dumpfile   = sys.argv[1];
_index      = 'econbiz'
_chunk_size = 10000;

_body = {
    '_op_type': 'index',
    '_index':   _index,
    '_id':      None,
    '_source':  None
}


# object mapping for [license.start.timestamp] tried to parse field [timestamp] as object, but found a concrete value

def remodel(doc):
    doc_ = dict();
    if 'open_access' in doc:
        doc_['oa'] = doc['open_access'] == 1;
    for fro,to in [ ('econbiz_id',              'id'),
                    ('date',                    'year'),
                    ('countryOfPublication',    'pub_countries'),
                    ('creator_personal',        'authors'),
                    ('language',                'languages'),
                    ('identifier_oclc',         'oclc_ids'),
                    ('identifier_isbn',         'isbns'),
                    ('identifier_doi',          'doi'),
                    ('identifier_url',          'urls'),
                    ('subject',                 'topics'),
                    ('subject_gnd',             'topics_gnd'),
                    ('subject_stw',             'topics_stw'),
                    ('subject_byAuthor',        'keywords'),
                    ('type_genre',              'genres'),
                    ('source_name',             'collection'),
                    ('contributor_corporate',   'institutions'),
                    ('title_alternative',       'alt_titles'),
                    ('publisher',               'publishers'),
                    ('isPartOf',                'sources'),
                    ('title',                   'title'),
                    ('type',                    'type'),
                    ('abstract',                'abstract')
                   ]:
        if fro in doc:
            doc_[to] = doc[fro];
    doc_['doi'] = doc_['doi'][0] if 'doi' in doc_ and isinstance(doc_['doi'],list) and len(doc_['doi'])>0 else None;
    doc         = {field:doc_[field] for field in doc_ if doc_[field]};
    return doc;

def load_econbiz(filename):
    IN   = open(filename);
    for line in IN:
        doc             = remodel(json.loads(line));
        body            = copy(_body);
        body['_id']     = doc['id'];
        body['_source'] = doc;
        yield body;
    IN.close();


_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,load_econbiz(_dumpfile),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % _chunk_size == 0:
        print('###############################',i,'###');
        _client.indices.refresh(index=_index);

