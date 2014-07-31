import MySQLdb as mdb
import argparse
import os
import csv 
import sys

def createDoc(doc):
        file = open(doc, "w")
        file.close()

def makeID():
        file = open("ID.txt", "w")
        first = str(0)
        file.write(first)
        file.close()

def uniTable(cur, table_char,dtb):
        cur.execute("SHOW TABLES FROM %s LIKE '%%%s%%'" % (dtb, table_char))
        val=cur.fetchall()
        table=val[0][0]
        return table

def uniCol(cur, table, col_char, type):
        cur.execute("SHOW COLUMNS FROM %s LIKE '%%%s%%'" % (table, col_char))
        val = cur.fetchall()
        for i in range(len(val)):
                if type in val[i][1]:
                        return val[i][0]

def writeArtID(cur, dtb):
	table = uniTable(cur, 'oc', dtb) 
	article = uniCol(cur, table, 'doi', 'varchar')
	cur.execute("SELECT %s FROM %s" % (article, table))
	rows=cur.fetchall()
	articles = []
	for i in range(len(rows)):
		artID = tup2str(rows[i])
		if '10.1146/' in artID:
			artID = artID.replace('10.1146/', '')
		else:
			artID = artID.replace('http://www.annualreviews.org/doi/abs/','')
		articles.append(artID)
	return articles

def nGramSearch(cur, keyword, dtb, doc):
	table = uniTable(cur, 'oc', dtb)
	docID = uniCol(cur, table, 'id', 'int')
	articleID = uniCol(cur, table, 'doi', 'varchar')
	articles = writeArtID(cur, dtb)
	file = open(doc, 'r')
	docList= file.readlines()
	file.close()
	artList = []
	file=open("error.txt", "w")
	for i in range(len(articles)):
		if os.path.isfile('/mnt/AnnualReviews/ngrams_flash0/%s.txt.csv' % articles[i]):
			f = open('/mnt/AnnualReviews/ngrams_flash0/%s.txt.csv' % articles[i], 'rb')
		elif os.path.isfile('/mnt/AnnualReviews/ngrams_flash0/%s.xml.csv' % articles[i]):
			f = open('/mnt/AnnualReviews/ngrams_flash0/%s.xml.csv' % articles[i], 'rb')
		elif os.path.isfile('/mnt/AnnualReviews/ngrams_flash1/%s.txt.csv' % articles[i]):
			f = open('/mnt/AnnualReviews/ngrams_flash1/%s.txt.csv' % articles[i], 'rb')
		elif os.path.isfile('/mnt/AnnualReviews/ngrams_flash1/%s.xml.csv' % articles[i]):
			f = open('/mnt/AnnualReviews/ngrams_flash1/%s.xml.csv' % articles[i], 'rb')
		elif os.path.isfile('/mnt/AnnualReviews/ngrams_flash2/%s.txt.csv' % articles[i]):
			f = open('/mnt/AnnualReviews/ngrams_flash2/%s.xml.csv' % articles[i], 'rb')
		else:
			try:
				f = open('/mnt/AnnualReviews/ngrams_flash2/%s.xml.csv' % articles[i], 'rb')
			except:
				e = sys.exc_info()[0]
				print(articles[i])

				file.write("%s\n" % articles[i])

		reader = csv.reader(f)
		for row in reader:
			if keyword in row:
				article	= '10.1146/'+articles[i]
				cur.execute("SELECT %s FROM %s WHERE %s = %s" % (docID, table, articleID, article))
				rows = cur.fetchall()
				for k in range(len(docList)):
					if tup2str(rows[0]) in docList[k]:
						continue;
				artList.append(tup2int(rows[0]))
	file.close()
	return artList		

def tup2int(tup):
        return int('.'.join(str(x) for x in tup))

def tup2str(tup):
        return str(','.join(str(x) for x in tup))

def addToDoc(cur, ID, dtb, doc, keyword, args = None):
	table = uniTable(cur,'oc',dtb)
	docId = uniCol(cur,table, 'id', 'int')
	title = uniCol(cur,table, 'itl', 'text')
	abstract = uniCol(cur,table, 'act', 'text')
        file = open(doc, "r")
        check = "Keyword: %s\n" % keyword
	checkID = "KeywordID: %d\n" % ID
        for line in file:
		if checkID == line:
			print("That is already a documented keyword")
			return
                if check == line:
                        print("That is already a documented keyword")
                        return
        file.close()
        file = open(doc, "a")
        cur.execute("SELECT %s FROM %s WHERE %s LIKE '%%%s%%' OR %s LIKE '%%%s%%'" % (docId, table, title, keyword, abstract, keyword))
        rows = cur.fetchall()
        for row in rows:
                if args==None:
			print("INSERTED: KEYWORDID: %d, KEYWORD: '%s' , DOCID: %d)" % (ID, keyword, tup2int(row)))
                	file.write("KeywordID: %d\nKeyword: %s\nDocID: %d\n  \n" % (ID, keyword, tup2int(row)))
		if args=="docID":
			file.write("%d\n" % tup2int(row))
		if args=="Keyowrd":
			file.write("%s\n" % keyword)
		if args=="KeywordID":
			file.write("%d\n" % ID)
#	artList= nGramSearch(cur, keyword, dtb, doc)
#	for i in range(len(artList)):
#		if args==None:
#			print("INSERTED: KEYWORDID: %d, KEYWORD: '%s' , DOCID: %d)" % (ID, keyword, tup2int(row)))
#			file.write("KeywordID: %d\nKeyword: %s\nDocID: %d\n  \n" % (ID, keyword, artList[i]))
#		if args=="docID":
#			file.write("%d\n" % artList[i])
        file.close()
        ID += 1
        Id = str(ID)
	if args ==None or args=="KeywordID":
        	file = open("ID.txt", "a")
        	file.write("%s\n" %Id)
        	file.close()

def makeUnion(doc, doc2):
	store=[]
        Idnum=[line.strip() for line in open(doc)]
        Idnum2=[line.strip() for line in open(doc2)]
        for i in range(len(Idnum)):
                for x in range(len(Idnum2)):
                        if Idnum[i]==Idnum2[x]:
                                store.append(Idnum[i])
	if len(store) > 0:
		file = open("union.txt", "w")
		for i in range(len(store)):
			file.write("%s\n" % store[i])
	

def writeOne(keywd, dtb, arg):
	dbs = mdb.connect(user = "root", passwd = "root", db = dtb)
	cur = dbs.cursor()
	ID = 0
	doc = "%s.txt" % keywd
	createDoc(doc)
	addToDoc(cur, ID, dtb, doc, keywd, args = arg)
	cur.close()
	dbs.close()

def writeTwo(keywd, keywd2, dtb, arg):
	dbs = mdb.connect(user = "root", passwd = "root", db = dtb)
        cur = dbs.cursor()
        ID = 0
	doc = "%s.txt" % keywd
	doc2 = "%s.txt" % keywd2
        createDoc(doc)
	createDoc(doc2)
        addToDoc(cur, ID, dtb, doc,  keywd, args = arg)
	addToDoc(cur, ID, dtb, doc2,  keywd2, args = arg)
        cur.close()
        dbs.close()

if __name__=='__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('keywrd')
	parser.add_argument('database')
	args = parser.parse_args()
	dbs = mdb.connect(user = "root", passwd = "root", db = args.database);
	cur = dbs.cursor()
	doc = "%s.txt" % args.keywrd
	if os.path.isfile(doc):
        	file = open("ID.txt", "r")
	        lineList = file.readlines()
	        file.close()
	        ID = int(lineList[-1])
		addToDoc(cur, ID, args.database, doc, args.keywrd)
		cur.close()
	        dbs.close()
	else:
	        createDoc(doc)
	        makeID()
	        file = open("ID.txt","r")
	        ID = int(file.read())
	        addToDoc(cur, ID, args.database, doc, args.keywrd)
		cur.close()
	        dbs.close()
