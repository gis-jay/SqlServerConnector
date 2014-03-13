import traceback, os, sys, logging
from datetime import datetime
from datetime import timedelta

###################################################################################################
###################################################################################################
#
# class:	DBUtil
# purpose:	Helper class that contains database helper methods
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################
class DBUtil(object):
	def __init__(self):
		return
		
	def getColumns(self, cursor):
		cols = cursor.description;
		columns = dict()
		for i in xrange(len(cols)):
			field_name = cols[i][0];
			columns[field_name] = i
		return columns
		
	def close(self, obj):
		if obj is not None:
			try:
				obj.close()
			except:
				None
				"""tb = sys.exc_info()[2]
				tbinfo = traceback.format_tb(tb)[0]
				msg = "Error in close:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
				logging.error(msg);"""

###################################################################################################
###################################################################################################
#
# class:	DateUtil
# purpose:	Helper class that contains Date utility methods
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################
class DateUtil(object):
	def __init__(self):
		return
		
	def ts(self, prefix = ""):
		return prefix + str(int(time.mktime(datetime.now().timetuple())))
		
	def now(self):
		return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	def tomorrow(self):
		t = datetime.now() + timedelta(days=1)
		return t.strftime('%Y-%m-%d %H:%M:%S')

###################################################################################################
###################################################################################################
#
# class:	LockFile
# purpose:	Helper class to determine if import process is running
#
# author:	Jason Sardano
# date:		Aug 20, 2013
#
###################################################################################################

class LockFile(object):
	def __init__(self, path):
		self._path = path
		
	def locked(self):
		return os.path.exists(self._path)
		
	def lock(self):
		try:
			with open(self._path, 'w') as f:
				logging.info("Writing lock file.")
				dateutil = DateUtil()
				f.write(dateutil.now())
		except Exception as e:
			logging.error('Error writing lock file')
			logging.exception(e)
		return
		
	def unlock(self):
		try:
			if os.path.exists(self._path):
				logging.debug('Removing lock file')
				os.remove(self._path)
		except Exception as e:
			logging.error('Error removing lock file')
			logging.exception(e)
		return
