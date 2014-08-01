import MySQLdb as mdb
import argparse
import os
import csv 
import sys

def createDoc(doc):
        file = open(doc, "w")
        file.close()

def makeID(): #This is important when running this program from the command line because it generates a keywordID
        file = open("ID.txt", "w")
        first = str(0)
        file.write(first)
        file.close()

def uniTable(cur, table_char,dtb): #This just allows for changing between databases with similar basic structure
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

def writeArtID(cur, dtb): #The ngrams are stored as either an .xml.csv or .txt.csv preceded by the doi of the article. Doi's in mysql are formatted differently, so this method is used to   				properly format them.
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

def nGramSearch(cur, keyword, dtb, doc): #The ngram search is long and tedious. It takes the keyword that is given, and the document that addToDoc() makes. Then, it takes the doi given by  					       the writeArtID() method and looks for the csv that correlates to that article. When it finds the article, it searches for the keyword in the file. 					    If it is found, it checks to make sure the doc Id correlating with the doi is not already stored, and, if it isn't, stores the docID. It has a very 					 long run time, so it is an optional method.
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

def addToDoc(cur, ID, dtb, doc, keyword, args = None, ngram = None): #This method, while somewhat long is actually pretty simple. All it does is accept an ID number for the keyword, the    									   doc name of where the docID should be saved, the keyword, and, if only the docID is necessary, a keyword. If you want 									ngrams, you can specify that too through optional arguments at the command line, or when you call writeOne() or 									     wirteTwo() from another program. Without the Ngrams, it just searches the Titles and Abstracts in the MySQL database, 									  searchign for the keyword. If found, it records the docID.
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
	if ngram is not None:	
		artList= nGramSearch(cur, keyword, dtb, doc)
		for i in range(len(artList)):
			if args==None:
				print("INSERTED: KEYWORDID: %d, KEYWORD: '%s' , DOCID: %d)" % (ID, keyword, tup2int(row)))
				file.write("KeywordID: %d\nKeyword: %s\nDocID: %d\n  \n" % (ID, keyword, artList[i]))
			if args=="docID":
				file.write("%d\n" % artList[i])
        file.close()
        ID += 1
        Id = str(ID)
	if args ==None or args=="KeywordID":
        	file = open("ID.txt", "a")
        	file.write("%s\n" %Id)
        	file.close()

def makeUnion(doc, doc2): #Make union is very simple. It takes two documents which contain the docID's for their separate keyword matches and sees if there are any docIDs that are in both.
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
	

def writeOne(keywd, dtb, arg, ngrams=None): #writeOne and write two should only be used by other programs when they call this one. It is not used at the command line.
	dbs = mdb.connect(user = "root", passwd = "root", db = dtb)
	cur = dbs.cursor()
	ID = 0
	doc = "%s.txt" % keywd
	createDoc(doc)
	if ngrams is not None:
		addToDoc(cur, ID, dtb, doc, keywd, args = arg, ngram=ngrams)
	else:
		addToDoc(cur, ID, dtb, doc, keywd, args = arg)
	cur.close()
	dbs.close()

def writeTwo(keywd, keywd2, dtb, arg, ngrams=None):
	dbs = mdb.connect(user = "root", passwd = "root", db = dtb)
        cur = dbs.cursor()
        ID = 0
	doc = "%s.txt" % keywd
	doc2 = "%s.txt" % keywd2
        createDoc(doc)
	createDoc(doc2)
	if ngrams is not None:
        	addToDoc(cur, ID, dtb, doc, keywd, args = arg, ngram=ngrams)
		addToDoc(cur, ID, dtb, doc2, keywd2, args = arg, ngram=ngrams)
	else: 
		addToDoc(cur, ID, dtb, doc, keywd, args = arg)
                addToDoc(cur, ID, dtb, doc2, keywd2, args = arg)
	cur.close()
        dbs.close()

if __name__=='__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('keywrd')
	parser.add_argument('database')
	parser.add_argument('--ngram')
	args = parser.parse_args()
	dbs = mdb.connect(user = "root", passwd = "root", db = args.database);
	cur = dbs.cursor()
	doc = "%s.txt" % args.keywrd
	if os.path.isfile(doc):
        	file = open("ID.txt", "r")
	        lineList = file.readlines()
	        file.close()
	        ID = int(lineList[-1])
		if args.ngram:
			addToDoc(cur, ID, args.database, doc, args.keywrd, ngram=true)
		else:
			addToDoc(cur, ID, args.database, doc, args.keywrd)
		cur.close()
	        dbs.close()
	else:
	        createDoc(doc)
	        makeID()
	        file = open("ID.txt","r")
	        ID = int(file.read())
	        if args.ngram:
                        addToDoc(cur, ID, args.database, doc, args.keywrd, ngram=true)
                else:
                        addToDoc(cur, ID, args.database, doc, args.keywrd)
		cur.close()
	        dbs.close()
