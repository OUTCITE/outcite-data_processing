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

_index = sys.argv[1];

_chunk_size       = 10000;
_scroll_size      = 10000;
_max_extract_time = 0.05;
_max_scroll_tries = 2;
_request_timeout  = 60;

_recheck = True;

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

INTEGER = re.compile(r'\b\d+\b');

# ---------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS-----------------------------------------------------------------------------------------------------------

def map_authors(index, authlist):
    authors = []
    if index == 'econbiz' or index == 'sowiport' or index == 'dnb' or index == 'ssoar' or index == 'gesis_bib' or index == 'research_data':
        for author in authlist:
            auth = author['name'] if index == 'econbiz' or index == 'ssoar' else author
            authsegs = auth.split(', ')
            authobj = {'author_string': '', 'surname': authsegs[0] if len(authsegs[0]) > 1 and authsegs[0][-1] != '.' else None, 'firstnames': [], 'initials': []}
            authfirstnames = authsegs[1].split() if len(authsegs) > 1 else []
            for fn in authfirstnames:
                if len(fn) > 1 and fn[-1] != '.':
                    authobj['firstnames'].append(fn)
                    authobj['initials'].append(fn[0])
                    authobj['author_string'] = fn if authobj['author_string'] == '' else ' '.join([authobj['author_string'], fn])
                else:
                    inits = fn.split('.')
                    authobj['initials'] = [string for string in inits if string.strip() != ""]
                    inits_string = ' '.join(authobj['initials'])
                    authobj['author_string'] = ' '.join([authobj['author_string'], inits_string]) if authobj['author_string'] != '' else inits_string
            authobj['author_string'] = ' '.join([authobj['author_string'], authsegs[0]]) if authobj['author_string'] != '' else authsegs[0]
            authors.append(authobj)

    elif index == 'openalex':
        for author in authlist:
            auth = author['author']['display_name'].strip() if 'display_name' in author['author'] and author['author']['display_name'] else ''
            if auth:
                authsegs = auth.split()
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

    elif index == 'crossref':
        for author in authlist:
            auth = author['family'] if 'family' in author and author['family'] else ''
            authobj = {'author_string': '', 'firstnames': [], 'initials': [], 'surname': auth}
            authfirstnames = author['given'].split() if 'given' in author and author['given'] else []
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
            authobj['author_string'] = ' '.join([authobj['author_string'], auth]) if authobj['author_string'] != '' else auth
            authors.append(authobj)

    return authors


def extract_ints(text):
    ints = [int(s) for s in INTEGER.findall(text)]
    return ints


def map_pages_info(plist):
    pstring = [str(p) for p in plist]
    return "-".join(pstring)


def collectionObj2modelObj(index, obj):
    # APA refstring format: Author's surname, Initials., (Date Published). Title. PublisherPlace: Publisher. Source. Volume(Issue), pp. pageStart-pageEnd. doi: doiString.
    refobj = {}
    src = obj['_source'] if '_source' in obj else None
    if src:
        if index == 'econbiz':
            # ['type', 'authors', 'year', 'title', 'publishers', 'sources']  # fields to be checked within an object
            if 'type' in src and src['type']:
                refobj['type'] = src['type']
            if 'authors' in src and len(src['authors']) != 0:
                refobj['authors'] = map_authors(index, src['authors'])
            if 'year' in src and src['year']:
                refobj['year'] = src['year']
            if 'title' in src and src['title']:
                refobj['title'] = src['title']
            if 'publishers' in src and len(src['publishers']) != 0:
                refobj['publishers'] = []
                for publisher in src['publishers']:
                    if 'place' in publisher:
                        refobj['place'] = publisher['place']
                    if 'name' in publisher:
                        refobj['publishers'].append({'publisher_string': publisher['name']})
            if 'sources' in src and len(src['sources']) != 0:
                for source in src['sources']:
                    if 'title' in source and source['title']:
                        refobj['source'] = source['title']
                    if 'volume' in source and source['volume']:
                        vols = extract_ints(source['volume'])
                        if len(vols) > 0:
                            refobj['volume'] = vols[0]
                    if 'issue' in source and source['issue']:
                        issue = extract_ints(source['issue'])
                        if len(issue) > 0:
                            refobj['issue'] = issue[0]
                    if 'pages' in source and source['pages']:
                        pages = extract_ints(source['pages'])
                        if len(pages) > 0:
                            refobj['start'] = pages[0]
                        if len(pages) > 1:
                            refobj['end'] = pages[1]
            if 'doi' in src and src['doi']:
                refobj['doi'] = src['doi']

        elif index == 'sowiport' or index == 'gesis_bib' or index == 'research_data':
            # ['subtype', 'person', 'date', 'title', 'publisher', 'source', 'doi']  # fields to be checked within each object
            if 'subtype' in src and src['subtype']:
                refobj['type'] = src['subtype']
            if 'person' in src and len(src['person']) != 0:
                refobj['authors'] = map_authors(index, src['person'])
            if 'date' in src and src['date']:
                if index == 'gesis_bib':
                    date = extract_ints(src['date'])
                    for d in date:
                        if len(str(d)) == 4:
                            refobj['year'] = d
                            break
                else:
                    refobj['year'] = int(src['date'])
            if 'title' in src and src['title']:
                title = src['title'][0] if isinstance(src['title'], list) else src['title']
                refobj['title'] = title
            if 'publisher' in src and src['publisher']:
                publisher = src['publisher'][0] if isinstance(src['publisher'], list) else src['publisher']
                refobj['publishers'] = [{'publisher_string': publisher}]
            if 'source' in src and src['source']:
                refobj['source'] = src['source']
            if 'doi' in src and src['doi']:
                doi = src['doi'][0] if isinstance(src['doi'], list) else src['doi']
                refobj['doi'] = doi

        elif index == 'openalex':
            # ['type', 'authorships', 'publication_year', 'title', 'host_venue', 'biblio', 'doi']  # fields to be checked within each object
            if 'type' in src and src['type']:
                refobj['type'] = src['type']
            if 'authorships' in src and len(src['authorships']) != 0:
                refobj['authors'] = map_authors(index, src['authorships'])
            if 'publication_year' in src and src['publication_year']:
                refobj['year'] = src['publication_year']
            if 'title' in src and src['title']:
                refobj['title'] = src['title']
            if 'host_venue' in src and src['host_venue'] and 'publisher' in src['host_venue'] and src['host_venue']['publisher']:
                refobj['publishers'] = [{'publisher_string': src['host_venue']['publisher']}]
            if 'biblio' in src:
                if 'volume' in src['biblio'] and src['biblio']['volume']:
                    vols = extract_ints(src['biblio']['volume'])
                    if len(vols) > 0:
                        refobj['volume'] = vols[0]
                if 'issue' in src['biblio'] and src['biblio']['issue']:
                    issues = extract_ints(src['biblio']['issue'])
                    if len(issues) > 0:
                        refobj['issue'] = issues[0]
                if 'first_page' in src['biblio'] and src['biblio']['first_page']:
                    pages = extract_ints(src['biblio']['first_page'])
                    if 'last_page' in src['biblio'] and src['biblio']['last_page'] and len(pages) == 1:
                        lpage = extract_ints(src['biblio']['last_page'])
                        if len(lpage) > 0:
                            pages.append(lpage[0])
                    if len(pages) > 0:
                        refobj['start'] = pages[0]
                    if len(pages) > 1:
                        refobj['end'] = pages[1]
            if 'doi' in src and src['doi']:
                refobj['doi'] = src['doi']

        elif index == 'crossref':
            # ['type', 'author', 'published-print', 'title', 'publisher', 'container-title', 'volume', 'page', 'DOI']  # fields to be checked within each object
            if 'type' in src and src['type']:
                refobj['type'] = src['type']
            if 'author' in src and len(src['author']) != 0:
                refobj['authors'] = map_authors(index, src['author'])
            if 'published-print' in src and 'date-parts' in src['published-print']:
                refobj['year'] = src['published-print']['date-parts'][0][0]
            if 'title' in src and len(src['title']) != 0:
                refobj['title'] = src['title'][0]
            if 'publisher' in src and src['publisher']:
                refobj['publishers'] = [{'publisher_string': src['publisher']}]
            if 'container-title' in src and len(src['container-title']) != 0:
                refobj['source'] = src['container-title'][0]
            if 'volume' in src and src['volume']:
                vols = extract_ints(src['volume'])
                if len(vols) > 0:
                    refobj['volume'] = vols[0]
            if 'issue' in src and src['issue']:
                issues = extract_ints(src['issue'])
                if len(issues) > 0:
                    refobj['issue'] = issues[0]
            if 'page' in src and src['page']:
                pages = extract_ints(src['page'])
                if len(pages) > 0:
                    refobj['start'] = pages[0]
                if len(pages) > 1:
                    refobj['end'] = pages[1]
            if 'DOI' in src and src['DOI']:
                refobj['doi'] = src['DOI']
                
        elif index == 'dnb':
            if 'authors' in src and len(src['authors']) != 0:
                refobj['authors'] = map_authors(index, src['authors'])
            if 'pub_dates' in src and len(src['pub_dates']) != 0:
                refobj['year'] = src['pub_dates'][0]
            if 'title' in src and src['title']:
                refobj['title'] = src['title']
            if 'pub_locs' in src and len(src['pub_locs']) != 0:
                refobj['place'] = src['pub_locs'][0]
            if 'publishers' in src and len(src['publishers']) != 0:
                refobj['publishers'] = [{'publisher_string': src['publishers'][0]}]
            if 'issn' in src and src['issn']:
                refobj['issn'] = src['issn']

        elif index == 'ssoar':
            # ['@type', 'authors', 'date_info', 'title', 'source_info', 'doi']  # fields to be checked within each object
            if '@type' in src and src['@type']:
                refobj['type'] = src['@type']
            if 'authors' in src and len(src['authors']) != 0:
                refobj['authors'] = map_authors(index, src['authors'])
            if 'date_info' in src and 'issue_date' in src['date_info'] and src['date_info']['issue_date']:
                refobj['year'] = int(src['date_info']['issue_date'])
            if 'title' in src and src['title']:
                refobj['title'] = src['title']
            if 'source_info' in src and src['source_info']:
                if 'src_journal' in src['source_info']:
                    refobj['source'] = src['source_info']['src_journal']
                if 'src_volume' in src['source_info']:
                    refobj['volume'] = int(src['source_info']['src_volume'])
                if 'src_issue' in src['source_info']:
                    refobj['issue'] = int(src['source_info']['src_issue'])
                if 'doi' in src and src['doi']:
                    refobj['doi'] = src['doi']

        elif index == 'arxiv':
            # ['authors_parsed', 'update_date', 'title', 'journal-ref', 'comments', 'doi']  # fields to be checked within each object
            if 'authors_parsed' in src and len(src['authors_parsed']) != 0:
                refobj['authors'] = src['authors_parsed']
                for author in refobj['authors']:
                    if 'surnames' in author:  # renaming surnames w.r.t our model
                        author['surname'] = author['surnames'] if author['surnames'] else ''
                        del author['surnames']
            if 'update_date' in src and src['update_date']:
                refobj['year'] = int(src['update_date'].split('-')[0])
            if 'title' in src and src['title']:
                refobj['title'] = src['title']
            if 'journal-ref' in src and src['journal-ref']:
                refobj['source'] = src['journal-ref']
            if 'doi' in src and src['doi']:
                refobj['doi'] = src['doi']

    return refobj


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
            obj                                           = collectionObj2modelObj(_index, doc);
            refstring                                     = json_to_apa(obj);
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
