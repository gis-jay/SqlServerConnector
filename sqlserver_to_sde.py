# notes:	Need to install 32-bit Python ODBC client (pyodbc), 64-bit doesn't work with ESRI's python installation
import os, sys, traceback
import logging, logging.handlers
from connector import util
from connector import db
from connector import io

def configure_logger(path):
	print('Logger writing to ' + path)
	msg_format = "%(asctime)s %(levelname)s \t %(message)s";
	logging.basicConfig(level=logging.DEBUG, format=msg_format)
	handler = logging.handlers.TimedRotatingFileHandler(path, 'D', 1, 30)
	formatter = logging.Formatter(msg_format);
	handler.setFormatter(formatter)
	logging.getLogger('').addHandler(handler);
	return;
	
def run(replicas):
	importer = io.SqlServerImporter(replicas)
	importer.run()
	return
		
if __name__ == "__main__":
	connectorConfig = None
	
	try:
		import config
		connectorConfig = config.connector
		configure_logger(connectorConfig['importLogFile'])
		logging.debug('Config file read successfully')
	except:
		tb = sys.exc_info()[2]
		tbinfo = traceback.format_tb(tb)[0]
		msg = "Error reading config file:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
		logging.error(msg);
	
	if connectorConfig is not None:
		try:
			replicaConfig = connectorConfig['replicas']
			replicas = db.Replicas(replicaConfig)
			run(replicas)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error parsing config file:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
	else:
		print('No config')