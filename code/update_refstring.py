# -IMPORTS-------------------------------------------------------------------------------------------------------------
import time
import json
import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
# ---------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS------------------------------------------------------------------------------------------------------

_index = sys.argv[1];

_chunk_size       = 1000;
_scroll_size      = 1000;
_max_extract_time = 0.5;
_max_scroll_tries = 2;
_request_timeout  = 60;

_recheck = False;

_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': { 'doc':  {
                                'has_refstring':       True,
                                'processed_refstring': True,
                                'refstr':              None
                                }
                     }
        }

_scr_query = {'bool':{'must_not':{'term':{'has_refstring': True}}}} if not _recheck else {'match_all':{}};

# ---------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS-----------------------------------------------------------------------------------------------------------

def map_authors(index, authlist):
    refstr = ''
    authors = []
    if index == 'econbiz' or index == 'sowiport':
        lastauth = authlist[-1]['name'] if index == 'econbiz' else authlist[-1]
        for author in authlist:
            auth = author['name'] if index == 'econbiz' else author
            authsegs = auth.split(', ')
            authobj = {'author_string': '', 'firstnames': [], 'initials': [], 'surname': authsegs[0] if len(authsegs[0]) > 1 and authsegs[0][-1] != '.' else None}
            authfirstnames = authsegs[1].split(' ') if len(authsegs) > 1 else []
            for fn in authfirstnames:
                if len(fn) > 0:
                    if fn[-1] != '.':
                        authobj['firstnames'].append(fn)
                        authobj['initials'].append(fn[0])
                        authobj['author_string'] = fn
                    else: # weird
                        authobj['initials'].append(fn[0])
                        authobj['author_string'] = ' '.join([authobj['author_string'], fn[0]]) if authobj['author_string'] != '' else fn[0]
            authobj['author_string'] = ' '.join([authobj['author_string'], authsegs[0]]) if authobj['author_string'] != '' else authsegs[0]
            authors.append(authobj)
            refstr = refstr + authobj['author_string'] + ', ' if auth != lastauth else refstr + authobj['author_string'] + ' '  # , is used as a delimiter for authors as complete name could be lastname, firstname

    elif index == 'openalex':
        lastauth = authlist[-1]['author']['display_name']
        for author in authlist:
            auth = author['author']['display_name']
            authsegs = auth.split(' ')
            authobj = {'author_string': '', 'firstnames': [], 'initials': [], 'surname': authsegs[-1]}
            authfirstnames = authsegs[:-1]
            for fn in authfirstnames:
                if len(fn) > 1 and fn[-1] != '.':
                    authobj['firstnames'].append(fn)
                    authobj['initials'].append(fn[0])
                    authobj['author_string'] = fn
                elif fn[-1] == '.':
                    inits = fn.split('.')
                    for i in inits:
                        if i:
                            authobj['initials'].append(i)
                            authobj['author_string'] = ' '.join([authobj['author_string'], i]) if authobj['author_string'] != '' else i
                else:
                    authobj['initials'].append(fn[0])
                    authobj['author_string'] = ' '.join([authobj['author_string'], fn[0]]) if authobj['author_string'] != '' else fn[0]
            authobj['author_string'] = ' '.join([authobj['author_string'], authsegs[-1]]) if authobj['author_string'] != '' else authsegs[-1]
            authors.append(authobj)
            refstr = refstr + authobj['author_string'] + ', ' if auth != lastauth else refstr + authobj['author_string'] + ' '

    elif index == 'crossref':
        lastauth = authlist[-1]['family']
        for author in authlist:
            auth = author['family']
            authobj = {'author_string': '', 'firstnames': [], 'initials': [], 'surname': author['family']}
            authfirstnames = author['given'].split(' ')
            for fn in authfirstnames:
                if len(fn) > 1 and fn[-1] != '.':
                    authobj['firstnames'].append(fn)
                    authobj['initials'].append(fn[0])
                    authobj['author_string'] = fn
                elif fn[-1] == '.':
                    inits = fn.split('.')
                    for i in inits:
                        if i:
                            authobj['initials'].append(i)
                            authobj['author_string'] = ' '.join([authobj['author_string'], i]) if authobj['author_string'] != '' else i
                else:
                    authobj['initials'].append(fn[0])
                    authobj['author_string'] = ' '.join([authobj['author_string'], fn[0]]) if authobj['author_string'] != '' else fn[0]
            authobj['author_string'] = ' '.join([authobj['author_string'], author['family']]) if authobj['author_string'] != '' else author['family']
            authors.append(authobj)
            refstr = refstr + authobj['author_string'] + ', ' if auth != lastauth else refstr + authobj['author_string'] + ' '

    return authors, refstr

def collectionObj2modelObj(index, obj):
    # general refstring format: [type] Author's names (Date Published) Title. PublisherPlace: Publisher. Editors. Source. Volume. Issue. pp. pageStart-pageEnd. doi: doiString.
    refstr = ''
    refobj = {}
    src = obj['_source'] if '_source' in obj else None
    try:
        if src:
            if index == 'econbiz':
                # ['type', 'authors', 'year', 'title', 'publishers', 'sources']  # fields to be checked within an object and appended to refstring in the given order
                if 'type' in src and src['type']:
                    refobj['type'] = src['type']
                if 'authors' in src and len(src['authors']) != 0:
                    refobj['authors'], refstr = map_authors(index, src['authors'])
                if 'year' in src and src['year']:
                    refstr = refstr + '(' + str(src['year']) + ') '
                    refobj['year'] = src['year']
                if 'title' in src and src['title']:
                    refstr = refstr + src['title'] + '. '
                    refobj['title'] = src['title']
                if 'publishers' in src and len(src['publishers']) != 0:
                    refobj['publishers'] = []
                    for publisher in src['publishers']:
                        if 'place' in publisher:
                            refstr = refstr + publisher['place']
                            refobj['place'] = publisher['place']
                            if 'name' in publisher:
                                refstr = refstr + ': ' + publisher['name'] + '. '
                                refobj['publishers'].append({'publisher_string': publisher['name']})
                        elif 'name' in publisher:
                            refstr = refstr + publisher['name'] + '. '
                            refobj['publishers'].append({'publisher_string': publisher['name']})
                if 'sources' in src and len(src['sources']) != 0:
                    for source in src['sources']:
                        if 'title' in source:
                            refstr = refstr + source['title'] + '. '
                            refobj['source'] = source['title']
                        if 'volume' in source:
                            vol = None;
                            try:
                                vol = int(source['volume'])
                            except:
                                print('Cannot get integer of',source['volume'])
                            if vol:
                                refobj['volume'] = vol
                                refstr = refstr + source['volume'] + '. '

                refobj['reference'] = refstr

            if index == 'sowiport':
                # ['type', 'person', 'date', 'title', 'publisher', 'source']  # fields to be checked within each object and appended to refstring in the given order
                if 'type' in src and src['type']:
                    refobj['type'] = src['type']
                if 'person' in src and len(src['person']) != 0:
                    refobj['authors'], refstr = map_authors(index, src['person'])
                if 'date' in src and src['date']:
                    refstr = refstr + '(' + src['date'] + ') '
                    refobj['year'] = int(src['date'])
                if 'title' in src and src['title']:
                    refstr = refstr + src['title'] + '. '
                    refobj['title'] = src['title']
                if 'publisher' in src and src['publisher']:
                    refstr = refstr + src['publisher'] + '. '
                    refobj['publishers'] = [{'publisher_string': src['publisher']}]
                if 'source' in src and src['source']:
                    refstr = refstr + src['source'] + '. '
                    refobj['source'] = src['source']
                refobj['reference'] = refstr

            if index == 'openalex':
                # ['type', 'authorships', 'publication_year', 'title', 'host_venue', 'biblio', 'doi']  # fields to be checked within each object and appended to refstring in the given order
                if 'type' in src and src['type']:
                    refobj['type'] = src['type']
                if 'authorships' in src and len(src['authorships']) != 0:
                    refobj['authors'], refstr = map_authors(index, src['authorships'])
                if 'publication_year' in src and src['publication_year']:
                    refstr = refstr + '(' + str(src['publication_year']) + ') '
                    refobj['year'] = src['publication_year']
                if 'title' in src and src['title']:
                    refstr = refstr + src['title'] + '. '
                    refobj['title'] = src['title']
                if 'host_venue' in src and 'publisher' in src['host_venue'] and src['host_venue']['publisher']:
                    refstr = refstr + src['host_venue']['publisher'] + '. '
                    refobj['publishers'] = [{'publisher_string': src['host_venue']['publisher']}]
                if 'biblio' in src:
                    if 'volume' in src['biblio'] and src['biblio']['volume']:
                        refstr = refstr + src['biblio']['volume'] + '. '
                        refobj['volume'] = int(src['biblio']['volume'])
                    if 'issue' in src['biblio'] and src['biblio']['issue']:
                        refstr = refstr + src['biblio']['issue'] + '. '
                        refobj['issue'] = int(src['biblio']['issue'])
                    if 'first_page' in src['biblio'] and src['biblio']['first_page']:
                        refstr = refstr + 'pp. ' + src['biblio']['first_page']
                        refobj['start'] = int(src['biblio']['first_page'])
                        if 'last_page' in src['biblio'] and src['biblio']['last_page']:
                            refstr = refstr + '-' + src['biblio']['last_page'] + '. '
                            refobj['end'] = int(src['biblio']['last_page'])
                        else:
                            refstr += '. '
                if 'doi' in src and src['doi']:
                    refstr = refstr + 'doi: ' + src['doi'] + '.'
                    refobj['doi'] = src['doi']
                refobj['reference'] = refstr

            if index == 'crossref':
                # ['type', 'author', 'published-print', 'title', 'publisher', 'container-title', 'volume', 'page', 'DOI']  # fields to be checked within each object and appended to refstring in the given order
                if 'type' in src and src['type']:
                    refobj['type'] = src['type']
                if 'author' in src and len(src['author']) != 0:
                    refobj['authors'], refstr = map_authors(index, src['author'])
                if 'published-print' in src and 'date-parts' in src['published-print']:
                    refstr = refstr + '(' + str(src['published-print']['date-parts'][0][0]) + ') '
                    refobj['year'] = src['published-print']['date-parts'][0][0]
                if 'title' in src and len(src['title']) != 0:
                    refstr = refstr + src['title'][0] + '. '
                    refobj['title'] = src['title']
                if 'publisher' in src and src['publisher']:
                    refstr = refstr + src['publisher'] + '. '
                    refobj['publishers'] = [{'publisher_string': src['publisher']}]
                if 'container-title' in src and len(src['container-title']) != 0:
                    refstr = refstr + src['container-title'][0] + '. '
                    refobj['source'] = src['container-title'][0]
                if 'volume' in src and src['volume']:
                    refstr = refstr + src['volume'] + '. '
                    refobj['volume'] = int(src['volume'])
                if 'page' in src and src['page']:
                    refstr = refstr + 'pp. ' + src['page'] + '. '
                    pages = src['page'].split('-')
                    refobj['start'] = int(pages[0]) if len(pages) > 0 and pages[0] else None
                    refobj['end'] = int(pages[1]) if len(pages) > 1 and pages[1] else None
                if 'DOI' in src and src['DOI']:
                    refstr = refstr + 'doi: ' + src['DOI'] + '.'
                    refobj['doi'] = src['DOI']
                refobj['reference'] = refstr

    except Exception as e:
        print(e)
        print(print('\n[!]-----> Some problem occurred while processing object: ' + obj + ' for index: ' + index))
    return refobj


def get_refstring():
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            obj                                           = collectionObj2modelObj(_index, doc);
            refstring                                     = obj['reference'] if 'reference' in obj else None;
            body                                          = copy(_body);
            body['_id']                                   = doc['_id'];
            body['_source']['doc']                        = doc['_source'];
            body['_source']['doc']['refstr']              = refstring;
            body['_source']['doc']['has_refstring']       = True if refstring else False;
            body['_source']['doc']['processed_refstring'] = True;
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
