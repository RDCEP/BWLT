#This program needs serious work. Currently, it does not map and its extraction is a bit janky. Sometimes it will extract full titles, sometimes it will only extract one or two words from a file without an apparent explanation. 

import MySQLdb as mdb
import keywordadd as ka
import os
import argparse
import csv

parser = argparse.ArgumentParser()
parser.add_argument('--keyword')
parser.add_argument('--keyword2')
parser.add_argument('database')
args= parser.parse_args()

dbs = mdb.connect(user='root', passwd='root', db = args.database)
cur = dbs.cursor()

def tup2int(tup):
        return int('.'.join(str(x) for x in tup))

def tup2str(tup):
        return str(','.join(str(x) for x in tup))

def keywordCrawl(filename):#crawl without a keyword will generate the web of inner citations throughout Annual Reviews. It will store all of the docIDs in the firstSet list, which it will then 			     iterate through in an attempt to find all of the titles of it's citations. If the citation exists, it then selects each individual work title from the larger list.  			      This is where the problems occur. For whatever reason, sometimes it will only extract a few words from a title. Future versions should fix this. When the title is   			       extracted it checks to see if that paper whas published in Annual reviews. If it is, the program records the source's docID and the citations docID onto a csv.      				Future versions should plot this on a map.              
	csvfile = open(filename, 'wb')
        writer = csv.writer(csvfile, delimiter=',')
	ka.writeOne(args.keyword, args.database, 'docID')
	file = open('%s.txt' % args.keyword, 'r')
	firstSet = file.readlines()
	secondSet = []
	for i in range(len(firstSet)):
		citation = []
		cur.execute("SELECT work_title FROM citations WHERE docID = %s" % firstSet[i])
		rows = cur.fetchall()
		for k in range(len(rows)):
			citation.append(tup2str(rows[k]))
		secondSet.append(citation)
		for j in range(len(secondSet)):
			citations = []
			for l in range(len(secondSet[j])):
				doc = ''.join([x if x != '\n' else '' for x in secondSet[j][l]])
				if doc != 'None':
					cur.execute('SELECT docID FROM docs WHERE Title LIKE "%%%s%%"' % doc)
					checks = cur.fetchall()
					if len(checks)>0:
						docID = tup2int(rows[0])
						writer.writerow(["%s"%firstSet[i]]+["%d"%docID])		

def crawl(filename):#crawl without a keyword will generate the web of inner citations throughout Annual Reviews. It will store all of the docIDs in the firstSet list, which it will then iterate		      through in an attempt to find all of the titles of it's citations. If the citation exists, it then selects each individual work title from the larger list. This is where   			the problems occur. For whatever reason, sometimes it will only extract a few words from a title. Future versions should fix this. When the title is extracted it checks to 			see if that paper whas published in Annual reviews. If it is, the program records the source's docID and the citations docID onto a csv. Future versions should plot this on			 a map.
	csvfile = open(filename, 'wb')
	writer = csv.writer(csvfile, delimiter=',')
	cur.execute("SELECT docID FROM docs")
	ids = cur.fetchall()
	firstSet = []
	for i in range(len(ids)):
		firstSet.append(tup2str(ids[i]))
        secondSet = []  
        for i in range(len(firstSet)):
                citation = [] 
                cur.execute("SELECT work_title FROM citations WHERE docID = %s" % firstSet[i])
                rows = cur.fetchall()
		if len(rows)==0:
			continue
                for k in range(len(rows)):
                        citation.append(tup2str(rows[k]))
                secondSet.append(citation)
        	for j in range(len(secondSet)):
                	citations = [] 
                	for l in range(len(secondSet[j])):
                        	doc = ''.join([x if x != '\n' else '' for x in secondSet[j][l]])
                        	if doc != 'None':
                                	cur.execute('SELECT docID FROM docs WHERE Title LIKE "%%%s%%"' % doc)
                                	checks = cur.fetchall()
                                	if len(checks)>0:
                                        	docID = tup2int(checks[0])
                                       		if docID !=3:
							print(docID,doc)
							writer.writerow(["%s"%firstSet[i]]+["%d"%docID])

if args.keyword:
	keywordCrawl('%scite.csv')
else:
	crawl('citations.csv')	
