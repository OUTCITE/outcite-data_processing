import sys, os
import json
import sqlite3


_dumpfile = sys.argv[1];
_dbfile   = sys.argv[2];

def extract_mapping(filename):
    IN = open(filename);
    i  = 0;
    for line in IN:
        i  += 1;
        if i % 100000 == 0:
            print(i,end='\r');
        doc = json.loads(line);
        doi = doc['doi'];
        for oa_location in doc['oa_locations']:
            best = oa_location['is_best'];
            url  = oa_location['url_for_pdf'] if oa_location['url_for_pdf'] else oa_location['url_for_landing_page'];
            yield (best,doi,url,);
    IN.close();

con = sqlite3.connect(_dbfile);
cur = con.cursor();

cur.execute("DROP   TABLE IF EXISTS doi2pdfs");
cur.execute("CREATE TABLE           doi2pdfs(best INT, doi TEXT, pdf_url TEXT)");

cur.executemany("INSERT INTO doi2pdfs VALUES (?,?,?)",(row for row in extract_mapping(_dumpfile)));

con.commit();

cur.execute("CREATE INDEX best_index ON doi2pdfs(best)");
cur.execute("CREATE INDEX doi_index  ON doi2pdfs(doi)");
cur.execute("CREATE INDEX url_index  ON doi2pdfs(pdf_url)");

con.close();
