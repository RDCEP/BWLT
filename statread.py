import MySQLdb as mdb
import itertools
import os
import keywordadd as ka
import matplotlib.pyplot as plt
import argparse
import collections

parser = argparse.ArgumentParser()
parser.add_argument('keyword')
parser.add_argument('--keyword2')
parser.add_argument('database')
parser.add_argument('--file')
parser.add_argument('--year')
parser.add_argument('--ngram')
args= parser.parse_args()


dbs = mdb.connect(user = "root", passwd = "root", db = args.database)
cur = dbs.cursor()
fig, ax = plt.subplots()

def uniTable(table_char): #uniTable(), and it's sister method uniCol(), serve as a way to help bridge the gap between mysql databases containing similar information, but are separately form				atted. There are limitations of course. For example, each database would need to have some level of similarity in order to be useful.
	cur.execute("SHOW TABLES FROM %s LIKE '%%%s%%'" % (args.database, table_char))
	val=cur.fetchall()
	table=val[0][0]
	return table

def uniCol(table, col_char, type):
	cur.execute("SHOW COLUMNS FROM %s LIKE '%%%s%%'" % (table, col_char))
	val = cur.fetchall()
	for i in range(len(val)):
		if type in val[i][1]:
			return val[i][0]

def tup2str(tup):
        return str(','.join(str(x) for x in tup))

def tup2int(tup):
        return int(','.join(str(x) for x in tup))

def plotUnion(lst):  #This method is designed takes a list as a parameter, which, in theory, contains the docIDs for all of the documents in which two keywords are located.The list past as 
	combine = [] #a parameter should have been created by reading from a text file created by the makeUnion() method in keywordadd.py. This will become more apparent later on in this
		     #program's code.
	table2=uniTable('utho')
	title=uniCol(table, 'itl', 'text')
	journal=uniCol(table, 'our', 'text')
	last=uniCol(table2, 'ast', 'varchar')
	first=uniCol(table2, 'irst', 'varchar')
	for i in range(len(lst)): #This loop is simple in concept. Following it, you'll realize that it is designed to simply search for the identificatiion information of each article.
		ident=[]
		ID=int(lst[i])
		cur.execute("SELECT %s, %s, %s FROM %s WHERE %s=%d" % (title, journal, year, table, docId,ID))
		info = cur.fetchall()
		ident.append(tup2str(info))
		cur.execute("SELECT %s, %s FROM %s WHERE %s=%d" % (last, first, table2, docId, ID))
		names= cur.fetchall()
		name = []
		for i in range(len(names)):
			name.append(tup2str(names[i]))
		ident.append(name)
		combine.append(ident)
	print(combine)
	file = open("%sand%s.txt"%(args.keyword, args.keyword2), "w")
	for item in combine:
		file.write("%s\n"%item)

def plotOne(arg): #plotOne() is designed to plot the frequency of a keyword across the journals contained in a database, or by the frequency throughout the years of publication for all of  			the journals combined.
	if args.ngram:
		ka.writeOne(args.keyword, args.database, "%s" % docId, ngram=true) #In plotOne, as well as plotTwo() a separate keyword hunter is used. This allows the plotting program to  											run more smoothly and offers other programs the ability to sift through keywords as well.
	else: 
		ka.writeOne(args.keyword, args.database, "%s" % docId)
	doc = "%s.txt" % args.keyword
	docnumbers = [line.strip() for line in open(doc)]
	stats = []
	names = cur.fetchall()
	for i in range(len(rows)):
		for x in range(len(docnumbers)):
			ID = int(docnumbers[x])
			cur.execute("SELECT %s FROM %s WHERE %s = %d" % (arg, table, docId, ID))
			check = cur.fetchall()
			if len(check) >0:

				if rows[i]==check[0] and rows[i][0] is None:
					stats.append(-1)
				elif rows[i]==check[0]:
					stats.append(tup2int(rows[i]))
	print(stats)
	rows_ = list(itertools.chain(*rows))
	if args.year:
		years=[stats.count(y) for y in rows_]
		plt.plot(rows_, years, marker='o', linestyle='--', color='r',label=args.keyword)
		labels=ax.set_xticklabels(rows_, rotation=90, fontsize='small')
		plt.ylim([0,max(years)])
	else:
		ax.hist(stats, bins = range(min(rows_), max(rows_)+2, 1), alpha=0.5, label = args.keyword)
	ax.legend(loc='upper right')
        ax.set_xticks(rows_)
	ax.set_xlabel("JournalID")
	ax.set_ylabel("Frequency")
	ax.set_title("Occurrences of %s" % args.keyword)
	if args.file:
		fig.savefig(args.file)
	else:
		fig.savefig("%s.png"%args.keyword)
	plt.show()	

def plotTwo(arg): #plotTwo() and plotOne() are very similar, but plotTwo() has a few more features and is a bit more robust. It follows much the same process as plotOne(), but plots an     			extra keyword as well as the union - if any exists - between the two keywords within a single document. Like plotOne(), plotTwo() needs to be passed an argument, which,  		     in this case is either the journalID or the publication year.
	if args.ngram:
		ka.writeTwo(args.keyword, args.keyword2, args.database, "%s" % docId, ngram=true)
	else:
		ka.writeTwo(args.keyword, args.keyword2, args.database, "%s" % docId)
	doc = "%s.txt" % args.keyword
	doc2 = "%s.txt" % args.keyword2
	ka.makeUnion(doc, doc2) #makeUnion() is a simple function that simply takes two documents and searches them for similar document numbers within each of them. If any unions exist,   				      they are saved in a document simply called union.txt
        docnumbers = [line.strip() for line in open(doc)]
        stats = []
	docnum = [line.strip() for line in open(doc2)]
	stats2 = []
	stats3 = []
	if os.path.isfile("union.txt"):
		docline = [line.strip() for line in open("union.txt")]
		for i in range(len(rows)):
			for z in range(len(docline)):
				ID = int(docline[z])
				cur.execute("SELECT %s FROM %s WHERE %s =%d" % (arg, table, docId, ID))
				check = cur.fetchall()
				if len(check) > 0:
					if rows[i]==check[0] and rows[i] is None:
 						stats.append(-1)
					elif rows[i]==check[0]:
						stats3.append(tup2int(rows[i]))
		os.remove("union.txt") #Once saved to a separate list, it's important to remove the document from whatever directory it was saved in order to prevent false positives with 					     other searches.
	for i in range(len(rows)):
                for y in range(len(docnum)):
                        ID = int(docnum[y])
                        cur.execute("SELECT %s FROM %s WHERE %s = %d" % (arg, table, docId, ID))
                        check = cur.fetchall()
                        if len(check) >0:
				if rows[i]==check[0] and rows[i] is None:
					stats.append(-1)
                                elif rows[i]==check[0]:
                                        stats2.append(tup2int(rows[i]))
                for x in range(len(docnumbers)):
                        ID = int(docnumbers[x])
                        cur.execute("SELECT %s FROM %s WHERE %s = %d" % (arg, table, docId, ID))
                        check = cur.fetchall()
                        if len(check) >0:
				if rows[i]==check[0] and rows[i] is None:
                                        stats.append(-1)	
                                elif rows[i]==check[0]:
                                        stats.append(tup2int(rows[i]))
	rows_ = list(itertools.chain(*rows)) #The global tuple "rows" is created by a cur.fetchall() call, which is demonstrated later. This tuple contains individual tuples and is not						   formatted in a way that matplotlib can easily access. Thus, itertools formats it to be more accessable for plotting. 
	if args.year:
		years=[stats.count(y) for y in rows_]
                plt.plot(rows_, years, marker='o', linestyle='--', color='r',label=args.keyword)
		years2 = [stats2.count(y) for y in rows_]
		plt.plot(rows_, years2, marker='o', linestyle='--', color='b', label=args.keyword2)
		if len(stats3):
			years3 = [stats3.count(y) for y in rows_]
			plt.plot(rows_, years3, marker='o', linestyle='--', color='g',label="%s and %s" %(args.keyword, args.keyword2))
		if len(years)>len(years2):
			plt.ylim([0, max(years)])
		elif len(years2)>=len(years):
			plt.ylim([0, max(years2)])
		labels=ax.set_xticklabels(rows_, rotation=90, fontsize='small')
	else:
		ax.hist(stats, bins = range(min(rows_), max(rows_)+2, 1), alpha=0.5, label = args.keyword)
		ax.hist(stats2, bins = range(min(rows_), max(rows_)+2, 1), alpha=0.5, label = args.keyword2)
      		if len(stats3):
			ax.hist(stats3, bins = range(min(rows_), max(rows_)+2, 1), alpha=0.5, label ="%s and %s"% (args.keyword, args.keyword2)) 
			plotUnion(stats3)
	ax.legend(loc='upper right')
        ax.set_xticks(rows_)
	if args.year:
                labels=ax.set_xticklabels(rows_, rotation=30, fontsize='small')
	ax.set_xlabel("JournalID")
        ax.set_ylabel("Frequency")
        ax.set_title("Occurrences of %s and %s" % (args.keyword, args.keyword2))
	if args.file:
                fig.savefig(args.file)
        else:
                fig.savefig("%sand%s.png"%(args.keyword, args.keyword2))
        plt.show()

def plotYear():
	if args.keyword2:
		plotTwo(year)
	else:
		plotOne(year)
	
if args.year:
	table=uniTable('oc')
        year=uniCol(table, 'ear', 'int')
	docId=uniCol(table, 'id', 'int')
	cur.execute("SELECT DISTINCT %s FROM docs WHERE %s IS NOT NULL ORDER BY %s ASC" % (year, year, year))
	rows=cur.fetchall()
	plotYear()
elif args.keyword2:
	table=uniTable('oc')
        year=uniCol(table, 'ear', 'int')
        docId=uniCol(table, 'id', 'int')
	jID = uniCol(table, 'our', 'int')
	cur.execute("SELECT DISTINCT %s FROM docs" % jID)
	rows=cur.fetchall()
	arg= jID
	plotTwo(arg)
else:
	table=uniTable('oc')
        year=uniCol(table, 'ear', 'int')
        docId=uniCol(table, 'id', 'int')
	jID = uniCol(table, 'our', 'int')
	cur.execute("SELECT DISTINCT %s FROM docs" % jID)
	rows = cur.fetchall()	
	arg = jID
	plotOne(arg)
