#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from collections import Counter
from copy import deepcopy as copy
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
#_infiles = ['/home/outcite/dnb/dnb_all_dnbmarc_20211013-1.mrc.xml','/home/outcite/dnb/dnb_all_dnbmarc_20211013-2.mrc.xml','/home/outcite/dnb/dnb_all_dnbmarc_20211013-3.mrc.xml','/home/outcite/dnb/dnb_all_dnbmarc_20211013-4.mrc.xml'];
#_infiles = ['/home/outcite/dnb/dnb_all_dnbmarc_20211013-3.mrc.xml','/home/outcite/dnb/dnb_all_dnbmarc_20211013-4.mrc.xml'];
_dumpfolder = sys.argv[1];
_upsert     = True if len(sys.argv) > 2 and sys.argv[2]=='upsert' else False;

_prefix     = '{http://www.loc.gov/MARC21/slim}';
_index      = 'dnb';#sys.argv[1];

_empty = ['',None,[]];

_body = { '_op_type': 'update' if _upsert else 'index',
          '_index': _index,
          '_id':    None,
          '_source': { 'id':           None,
                       'ids':          [],
                       'pub_locs':     [],
                       'pub_dates':    [],
                       'publishers':   [],
                       'countries':    [],
                       'isbn':         None,
                       'issn':         None,
                       'title':        None,
                       'subtitle':     None,
                       'titles':       [],
                       'authors':      [],
                       'editors':      [],
                       'contributors': []
                     } if not _upsert else { 'doc': {  'id':           None,
                                                       'ids':          [],
                                                       'pub_locs':     [],
                                                       'pub_dates':    [],
                                                       'publishers':   [],
                                                       'countries':    [],
                                                       'isbn':         None,
                                                       'issn':         None,
                                                       'title':        None,
                                                       'subtitle':     None,
                                                       'titles':       [],
                                                       'authors':      [],
                                                       'editors':      [],
                                                       'contributors': []
                                                    },
                                             'doc_as_upsert': True
                                           }
        };

_target = {
            'ids':          [],
            'pub_locs':     [],
            'pub_dates':    [],
            'publishers':   [],
            'countries':    [],
            'isbn':         None,
            'issn':         None,
            'title':        None,
            'subtitle':     None,
            'titles':       [],
            'persons':      [],
            'roles':        []
          };

_used_info = {  '015': { '_name': 'DNB ID',
                          'a':     { '_name': 'DNB ID',       '_to': ['ids']               }},
                '035': { '_name': 'ID',
                          'a':     { '_name': 'ID',           '_to': ['ids']               }},
                '264': { '_name': 'pub info',
                          'a':     { '_name': 'pub loc',      '_to': ['pub_locs']          },
                          'b':     { '_name': 'publisher',    '_to': ['publishers']        },
                          'c':     { '_name': 'pub date',     '_to': ['pub_dates']         }},
                '044': { '_name': 'country',
                          'a':     { '_name': 'MARC code',    '_to': ['countries']         },
                          'b':     { '_name': 'local code',   '_to': ['countries']         },
                          'c':     { '_name': 'ISO code',     '_to': ['countries']         }},
                '020': { '_name': 'ISBN',
                          'a':     { '_name': 'ISBN',         '_to': ['isbn']              }},
                '022': { '_name': 'ISSN',
                          'a':     { '_name': 'ISSN',         '_to': ['issn']              }},
                '245': { '_name': 'title statement',
                          'a':     { '_name': 'title',        '_to': ['title','titles']    },
                          'b':     { '_name': 'subtitle',     '_to': ['subtitle','titles'] }},
                '246': { '_name': 'alterantive title',
                          'a':     { '_name': 'alt title',    '_to': ['titles']            },
                          'b':     { '_name': 'alt subtitle', '_to': ['titles']            }},
                '247': { '_name': 'old title',
                          'a':     { '_name': 'old title',    '_to': ['titles']            },
                          'b':     { '_name': 'old subtitle', '_to': ['titles']            }},
                '210': { '_name': 'abbreviated title',
                          'a':     { '_name': 'abbrev title', '_to': ['titles']            }},
                '222': { '_name': 'key title',
                          'a':     { '_name': 'key title',    '_to': ['titles']            }},
                '100': { '_name': 'person',
                          'a':     { '_name': 'person name',  '_to': ['persons']           },
                          '4':     { '_name': 'role',         '_to': ['roles']             }},
             };

_counter = Counter();
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def add(content,info_code,attr_code,obj):
    for target_field in _used_info[info_code][attr_code]['_to']:
        if isinstance(obj[target_field],list):
            obj[target_field].append(content);
        else:
            obj[target_field] = content;
    return obj;

def transform(obj):
    body              = copy(_body);
    #ID                = hash('#'.join(sorted(obj['ids']))); #TODO: Does this really create the same hash given the same input ID, even when computer has changed?
    #ID                = '0'+str(ID) if ID >= 0 else '1'+(str(ID)[1:]);
    ID                = '-'.join(sorted(obj['ids'])).replace('(','').replace(')','-').replace(',','-');
    body['_id']       = ID;
    doc               = body['_source'] if not _upsert else body['_source']['doc'];
    doc['id']         = ID;
    doc['ids']        = obj['ids'];
    doc['isbn']       = obj['isbn'];
    doc['issn']       = obj['issn'];
    doc['title']      = obj['title'];
    doc['pub_locs']   = list(set(obj['pub_locs']));
    doc['pub_dates']  = list(set(obj['pub_dates']));
    doc['countries']  = list(set(obj['countries']));
    doc['publishers'] = obj['publishers'];
    roles                         = [(obj['roles'][i],obj['persons'][i],) for i in range(len(obj['persons']))];
    for role,name in roles:
        if role == 'aut' and not name in doc['authors']: #TODO: Warning: although these lists are very short, this is searching in a list!
            doc['authors'].append(name);
        elif role == 'edt' and not name in doc['editors']:
            doc['editors'].append(name);
        elif role == 'pbl' and not name in doc['publishers']:
            doc['publishers'].append(name);
        elif role == 'ctb' and not name in doc['contributors']:
            doc['contributors'].append(name);
    keys = list(doc.keys());
    for key in keys:
        if doc[key] in _empty:
            del doc[key];
    for key in doc:
        if _upsert:
            body['_source']['doc'][key] = doc[key];
        else:
            body['_source'][key] = doc[key];
    return body;

def valid(obj):
    if isinstance(obj,dict) and 'title' in obj and not obj['title'] in _empty and 'ids' in obj and isinstance(obj['ids'],list) and len(obj['ids']) > 0:
        return True;
    return False;

def parse(infile):
    global _counter
    IN          = open(infile);
    level       = 0;
    current_obj = None;
    for event, elem in ET.iterparse(IN, events=('start','end')):
        if event == 'start':
            level += 1
            if level == 2:                                                  #--------------Below is one record at this point
                #-----------------------------------------------------------------------------------------------------------
                #if current_obj and len(current_obj['persons']) > 1:
                #    print('.................................................................');
                #    for key in current_obj:
                #        print(key,':',current_obj[key]);
                if valid(current_obj):
                    yield transform(current_obj);
                current_obj = copy(_target);
                for child in elem:
                    if child.tag==_prefix+'datafield':
                        info_code = child.attrib['tag'];
                        if info_code in _used_info:
                            for entry in child:
                                attr_code = entry.attrib['code'];
                                if attr_code in _used_info[info_code]:
                                    current_obj = add(entry.text,info_code,attr_code,current_obj);
                        else:
                            _counter[info_code] += 1;
                            #print('#'+str(counter[info_code]),info_code,':',[(entry.attrib['code'],entry.text,) for entry in child]);
                #------------------------------------------------------------------------------------------------------------
        if event == 'end':
            level -= 1;
        elem.clear();
    IN.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

for infile in os.listdir(_dumpfolder):
    if not infile.endswith('.mrc.xml'):
        print(infile,'not an mrc.xml file. Skipping...');
        continue;
    print(infile);
    i = 0;
    for success, info in bulk(client,parse(_dumpfolder+infile)):
        i += 1;
        if not success:
            print('A document failed:', info['index']['_id'], info['index']['error']);
        elif i % 10000 == 0:
            print(i,end='\r');
            client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
