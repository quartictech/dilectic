#!/bin/env python 
import csv
import zipfile
import sys
import psycopg2
from datetime import datetime
import lxml
from lxml import etree
from lxml.cssselect import CSSSelector
import lxml.html
import psycopg2.extras

def flatten_text(el):
  if el.text: return el.text
  elif len(el) > 0:
    return el[0].text

def contains_text(el, text):
  if el.text and text in el.text.lower(): return True
  elif len(el) > 0:
    for child in el:
      if child.text and text in child.text.lower(): return True
  return False
    
def parse_date(s):
   if not s:
     return None
   try:
     return datetime.strptime(s, '%d/%m/%Y') 
   except:
     raise ValueError("Can't parse " + s)

def find_where(l, prefix):
  result = []
  found_idx = None
  for idx, val in enumerate(l):
    if val.startswith(prefix):
      result.append(val[len(prefix):].strip().rstrip())
      found_idx = idx + 1
      break
  while found_idx and found_idx < len(l) and l[found_idx]:
    result.append(l[found_idx].strip().rstrip())
    found_idx += 1
  return result
  

conn = psycopg2.connect('host=localhost dbname=dilectic user=dilectic password=dilectic98')
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS accounts(
	id SERIAL, 
	CompanyNumber VARCHAR,
        date DATE,
	data JSONB,
	raw JSONB,
 	source VARCHAR
	)""")


namespaces = {'ix': 'http://www.xbrl.org/2008/inlineXBRL', 'xhtml': 'http://www.w3.org/1999/xhtml'}
sel_metadata = CSSSelector('ix|header ix|hidden ix|nonNumeric', namespaces=namespaces)
sel_accounts = CSSSelector('xhtml|table.iris_table', namespaces=namespaces)
sel_non_fractions = CSSSelector('ix|nonFraction', namespaces=namespaces)
print(sel_non_fractions.path)

for f in sys.argv[1:]:
  count = 0
  zip = zipfile.ZipFile(f)
  names = list(zip.namelist())
  print("%s : %d files" % (f, len(names)))
  for name in names:
    #print(name)
    date_str = name.split('_')[3].split('.')[0]
    file = zip.open(name)
    tree = etree.parse(file)
    count += 1

    doc = {}

    for element in sel_metadata(tree):
      #print(element)
      doc[element.get("name")] = element.text

    lines = []
    for element in sel_accounts(tree):
      #print(element)
      s = etree.tostring(element, method="text", encoding="utf-8").decode()
      s = ' '.join(s.split())
      lines.append(s)

    for element in sel_non_fractions(tree):
      #print(element)
      doc[element.get("name")] = {"contextRef": element.get('contextRef'), "unitRef": element.get('unitRef'), "value": element.text}

    directors = find_where(lines, 'DIRECTORS:') 
    registered_number = find_where(lines, 'REGISTERED NUMBER:')
    if registered_number:
      registered_number = registered_number[0].split()[0]


    doc['directors'] = directors
    date = datetime.strptime(date_str, '%Y%m%d')

    #print(doc)

    cur.execute('INSERT INTO accounts(CompanyNumber, date, data, raw, source) VALUES(%s, %s, %s, %s, %s)', (registered_number, date, psycopg2.extras.Json(doc), psycopg2.extras.Json(lines), f + ':' + name))

    if count % 10000 == 0:
      print(count)
      conn.commit()
