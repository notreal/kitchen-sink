import psycopg2
import sys
from multiprocessing import Pool

# removed sensitive info, copy/edit to make useful

if len(sys.argv) != 4:
	print "Usage: python multithreaded_postgres.py arg1 arg2 arg3"
	sys.exit(1)
table_name = "tt_" + sys.argv[1]
start_date = sys.argv[2]
end_date   = sys.argv[3]

def worker(arg):
	"does the work"
	query = "select a_function('{0}','{1}','{2}',null,null); insert into {3} select * from a_table;".format(arg, start_date, end_date, table_name)
	w_con = psycopg2.connect("dbname=postgres")
	w_cur = w_con.cursor()
	w_cur.execute(query)
	print "Finished " + arg
	w_con.commit()
	w_cur.close()
	w_con.close()

con = None

try:
	print "Connecting to database"
	con = psycopg2.connect("dbname=postgres")
	cur = con.cursor()
	
	cur.execute("""
		select 1;
	""")
	
	things = cur.fetchall()
	if len(things) <= 0:
		print "No things found for date range"
		sys.exit(1)
	cur.close()
	
	# create results table without having to worry about column types
	print "Creating {0}, it will be dropped if it already exists.".format(table_name)
	cur2 = con.cursor()
	cur2.execute("drop table if exists {0};".format(table_name))
	cur2.execute("select a_function('foo_bar','2014-01-01','2014-01-02',null,null);")
	cur2.execute("create unlogged table {0} as select * from a_table;".format(table_name))
	cur2.execute("grant select on {0} to public;".format(table_name))
	con.commit()
	cur2.close()

	pool = Pool(processes=12)

	# main loop
	for thing in things:
		pool.apply_async(worker, [thing[0]])
	
	pool.close()
	pool.join()
	
	print "Results will be in {0}".format(table_name)

except psycopg2.DatabaseError, e:
	print 'Error: %s' % e
	sys.exit(1)

finally:
	if con:
		con.close()
