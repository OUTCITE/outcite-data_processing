# -IMPORTS-------------------------------------------------------------------------------------------------------------
import time
import json
import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import re
# ---------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS------------------------------------------------------------------------------------------------------

_index = 'cioffi';

_chunk_size       = 10;
_scroll_size      = 10;
_max_extract_time = 0.05;
_max_scroll_tries = 2;
_request_timeout  = 60;

_body = { '_op_type': 'update', #TODO: Needs be changed, we are iterating over references in this script instead
          '_index': _index,
          '_id': None,
          '_source': { 'doc':  {
                                'gold_refobjects': None,
                                'gold_refstrings': None
                                }
                     }
        }

_scr_query = {'bool':{'must':{'term':{'has_gold_refobjects': True}}}};

# ---------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS-----------------------------------------------------------------------------------------------------------

def json_to_apa(json_obj):
    # Get the authors, year, title, publishers, place, journal, volume, issue, start and end page values from the JSON object
    authors = json_obj.get('authors', [])
    author_list = [f"{author['surname'] + ', ' if 'surname' in author and author['surname'] else ''}{'. '.join(author['initials']) if 'initials' in author else ''}." for author in authors] if len(authors) > 0 else []
    author_string = ', '.join(author_list)
    year = json_obj.get('year', 'n.d.')
    title = json_obj.get('title', '')
    publishers = json_obj.get('publishers', [])
    publisher_string = publishers[0]['publisher_string'] if len(publishers) > 0 else ''
    place = json_obj.get('place', '')
    journal = json_obj.get('source', '')
    volume = json_obj.get('volume', '')
    issue = json_obj.get('issue', '')
    start_page = json_obj.get('start', '')
    end_page = json_obj.get('end', '')
    doi = json_obj.get('doi', '')

    # Construct the reference string in APA format
    space = ' '
    if author_string:
        reference_string = f"{author_string}{' (' + str(year) + ').'}{space + title + '.' if title else ''}{space + place + ':' if place else ''}{space + publisher_string + '.' if publisher_string else ''}{space + journal + ',' if journal else ''}{space + str(volume) if volume else ''}{'(' + str(issue) + '),' if issue else ''}{' pp. ' + str(start_page) if start_page else ''}{'-' + str(end_page) + '.' if end_page else ''}{' doi: ' + doi + '.' if doi else ''}"
    else:
        reference_string = f"{title + '.' if title else ''}{' (' + str(year) + ').'}{space + place + ':' if place else ''}{space + publisher_string + '.' if publisher_string else ''}{space + journal + ',' if journal else ''}{space + str(volume) if volume else ''}{'(' + str(issue) + '),' if issue else ''}{' pp. ' + str(start_page) if start_page else ''}{'-' + str(end_page) + '.' if end_page else ''}{' doi: ' + doi + '.' if doi else ''}"

    reference_string = reference_string.rstrip()  # remove trailing whitespace
    reference_string = reference_string[:-1] if reference_string[-2:] == '..' or reference_string[-1] == ',' else reference_string  # remove extra dot(.) or comma(,) from end of reference string
    reference_string = reference_string + '.' if reference_string[-1] != '.' else reference_string  # end with dot
    return reference_string


def get_refstring():
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            body                                      = copy(_body);
            body['_id']                               = doc['_id'];
            body['_source']['doc']['gold_refstrings'] = [];
            body['_source']['doc']['gold_refobjects'] = copy(doc['_source']['gold_refobjects']);
            for i in range(len(body['_source']['doc']['gold_refobjects'])):
                refstring                                                            = json_to_apa(body['_source']['doc']['gold_refobjects'][i]);
                body['_source']['doc']['gold_refobjects'][i]['reference'] = refstring;
                body['_source']['doc']['gold_refstrings'].append(refstring);
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
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

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_refstring(),chunk_size=_chunk_size,request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
