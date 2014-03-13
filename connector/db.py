import os, sys, arcpy, uuid
import traceback
import logging
import pyodbc

import util

###################################################################################################
###################################################################################################
#
# class:	db.Replicas
# purpose:	Class that parses a Python dictionary and extract an array of replicas.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
# notes:	Need to install 32-bit Python ODBC client (pyodbc), 64-bit doesn't work with ESRI's python installation
#
###################################################################################################

class Replicas(object):
	#replicaConfigs: An array of Replica configs, see Replica below.
	def __init__(self, replicaConfigs):
		self.replicas = []
		for i in range(0, len(replicaConfigs)):
			replicaConfig = replicaConfigs[i]
			replica = Replica(replicaConfig)
			if replica.disabled:
				logging.info(replica.name + ' is disabled')
			else:
				logging.info('Adding ' + replica.name)
				self.replicas.append(replica)
		return
		
###################################################################################################
###################################################################################################
#
# class:	db.Replica
# purpose:	Class that parses a Python dictionary, extracts information about the replica,
#			and extracts an array of datasets.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
###################################################################################################

class Replica(object):
	def __init__(self, config):
		self.name = config['name']
		self.datasets = []
		
		if 'disabled' in config:
			self.disabled = config['disabled']
		else:
			self.disabled = False
		
		self.tempPath = config['tempPath']
		self.exportPath = config['exportPath']
		self.lockFilePath = config['lockFilePath']
		self.deleteTempFiles = config['deleteTempFiles']
		self.autoReconcile = config['autoReconcile']
		self.stagingWorkspace = config['stagingWorkspace']
		self.productionWorkspace = config['productionWorkspace']
		self.sqlserverEditVersion = config['sqlserverEditVersion']
		self.stagingEditVersions = config['stagingEditVersions']
		self.stagingDefaultVersion = config['stagingDefaultVersion']
		
		server = config['sqlServer']['server']
		database = config['sqlServer']['database']
		self._connectionString = "DRIVER={SQL Server};SERVER=${server};DATABASE=${database};Trusted_Connection=yes".replace('${server}', server).replace('${database}', database)
		
		self._connection = None
		self.dbutil = util.DBUtil()

		if not self.disabled:
			for i in range(0, len(config['datasets'])):
				dataset = Dataset(config['datasets'][i], self)
				if dataset.disabled:
					logging.info(str(dataset) + ' is disabled.')
				else:
					self.datasets.append(dataset)
		return
		
	def __del__(self):
		if self._connection:
			self.close(self._connection)
		return
		
	def __str__(self):
		return self.name
		
	def close(self, dbobject):
		if self.dbutil and dbobject is not None:
			self.dbutil.close(dbobject)
		return
		
	def isConnected(self):
		return not self._connection is None
		
	def connect(self):
		func = 'Replica._connect'
		try:
			self._connection = pyodbc.connect(self._connectionString)
			return True
		except:
			self._connection = None
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return False
		
	def closeConnection(self):
		if self.isConnected():
			try:
				self._connection.close()
				self._connection = None
			except:
				tb = sys.exc_info()[2]
				tbinfo = traceback.format_tb(tb)[0]
				msg = "Error in close:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
				logging.error(msg);
		return
		
	def getConnection(self):
		return self._connection

###################################################################################################
###################################################################################################
#
# class:	db.Dataset
# purpose:	Class that parses a Python dictionary and extracts information about the dataset.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
###################################################################################################

class Dataset(object):
	def __init__(self, config, replica):
		self.replica = replica
		if 'disabled' in config:
			self.disabled = config['disabled']
		else:
			self.disabled = False
		self.cdcFunction = config['cdcFunction']
		self.cdcTable = config['sqlserverDataset']['table']
		self.cdcPrimaryKey = config['sqlserverDataset']['primaryKey']
		self.isSpatial = ('xField' in config['sqlserverDataset']) and ('yField' in config['sqlserverDataset'])
		if self.isSpatial:
			self.xField = config['sqlserverDataset']['xField']
			self.yField = config['sqlserverDataset']['yField']
		self.sdeTable = config['sdeDataset']['table']
		self.sdePrimaryKey = config['sdeDataset']['primaryKey']
		
		self._changeCursor = None
		self._changeCursorFields = None
		
		return
		
	def __str__(self):
		return self.cdcTable + '->' + self.sdeTable;
		
	########################################################################
	# Executes the CDC function and returns a cursor of CDC records for the dataset.
	def getChanges(self):
		func = 'Dataset.getChanges'
		sql = ''
		try:
			self.replica.close(self._changeCursor)
			self._changeCursorFields = None
			
			if not self.replica.isConnected():
				logging.error(func + ': No connection')
				return None
		
			dateUtil = util.DateUtil()
			now = dateUtil.now()
			self._changeCursor = self.replica.getConnection().cursor()
		
			logging.debug('Calling CDC function ' + self.cdcFunction)
			sql = '''
			DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);
SET @begin_time = \'2001-01-01 00:00:01\';
SET @end_time = \'''' + now + '''\';
SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
SELECT *, CONVERT(VARCHAR(MAX), __$seqval, 2) as __$CDCKEY FROM ''' + self.cdcFunction + '''(@begin_lsn, @end_lsn, 'all');
'''
			try:
				self._changeCursor.execute(sql)
			except:
				logging.warn('Error calling CDC function. Dataset probably has no CDC changes.')
				return None;
				
			self._changeCursorFields = self.replica.dbutil.getColumns(self._changeCursor)
			
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
			return None
		
		return self._changeCursor
		
	def getChangeFields(self):
		return self._changeCursorFields
		
	########################################################################
	# Determine the database operation type of the row.
	# returns "insert","update","delete"
	def getOperationType(self, cdcRow):
		func = "Dataset.getOperationType"
		op = "";
		try:
			operation = cdcRow[self._changeCursorFields["__$operation"]]
			if operation == 1:
				op = "delete"
			elif operation == 2:
				op = "insert"
			elif operation == 4:
				op = "update"
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return op
	
	########################################################################
	# Deletes records from the CDC table of records that were processed.
	# processedRecords: An array of CDC IDs
	def clearChanges(self, processedRecords):
		logging.info('Clearing changes from CDC tables for ' + self.cdcTable)
		func = "Database.clearChanges"
		try:
			cursor = self.replica.getConnection().cursor()
			ids = ""
			for i in range(0, len(processedRecords)):
				if i > 0:
					ids = ids + ","
				ids = ids + "'" + processedRecords[i] + "'"
			sql = 'DELETE FROM ' + self.cdcTable + ' where CONVERT(VARCHAR(MAX), __$seqval, 2) in (' + ids + ')'
			cursor.execute(sql)
			self.replica.getConnection().commit()
			logging.debug('Deleted ' + str(cursor.rowcount) + ' rows from ' + self.cdcTable)
			cursor.close()
			del cursor
			cursor = None
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return
		
	def getSdeTablePath(self):
		return os.path.join(self.replica.stagingWorkspace, self.sdeTable)

	def makeLayer(self, key):
		where_clause = self.sdePrimaryKey + " = " + "'" + str(key) + "'"
		return self.makeLayerFromQuery(where_clause)
		
	def makeLayerFromQuery(self, where_clause):
		feature_class = self.getSdeTablePath()
		layer_name = "lyr" + str(uuid.uuid1()).replace("-", "")
		if self.isSpatial:
			arcpy.MakeFeatureLayer_management(feature_class, layer_name, where_clause)
			logging.debug('Changing version to Desktop')
			arcpy.ChangeVersion_management(layer_name,'TRANSACTIONAL', 'DBO.DESKTOP','')
			logging.debug('Changing version to BG-BASE')
			arcpy.ChangeVersion_management(layer_name,'TRANSACTIONAL', self.replica.sqlserverEditVersion,'')
		else:
			arcpy.MakeTableView_management(feature_class, layer_name, where_clause)
		return layer_name
		
	def logBgBaseInfo(self, feature, cdcRow):
		func = 'logBgBaseInfo'
		logging.debug(' ')
		logging.debug('Logging BG-BASE info for record:')
		try:
			fields = self.getChangeFields()
			self._logBgBaseInfo('ACC_NUM_AND_QUAL', feature, cdcRow, fields)
			self._logBgBaseInfo('rep_id', feature, cdcRow, fields)
			self._logBgBaseInfo('line_seq', feature, cdcRow, fields)
			self._logBgBaseInfo('replication_tms', feature, cdcRow, fields)
			self._logBgBaseInfo('replication_action_cde', feature, cdcRow, fields)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
			
		logging.debug(' ')
		return
			
	def _logBgBaseInfo(self, field_name, feature, cdcRow, fields):
		msg = field_name
		if feature is not None and cdcRow is not None:
			sdeval = feature.getValue(field_name)
			cdcval = cdcRow[fields[field_name]]
			
			if sdeval is not None and cdcval is not None:
				msg = msg + ' SDE: ' + str(sdeval)
				msg = msg + ', CDC: ' + str(cdcval)
			elif sdeval is None and cdcval is None:
				msg = msg + ' SDE: Null'
				msg = msg + ', CDC: Null'
			elif sdeval is not None:
				msg = msg + ' SDE: ' + str(sdeval)
				msg = msg + ', CDC: Null'
			else:
				msg = msg + ' SDE: Null'
				msg = msg + ', CDC: ' + str(cdcval)
		elif feature is not None:
			sdeval = feature.getValue(field_name)
			if sdeval is not None:
				msg = msg + ' SDE: ' + str(sdeval)
			else:
				msg = msg + ' SDE: Null'
		elif cdcRow is not None:
			cdcval = cdcRow[fields[field_name]]
			if cdcval is not None:
				msg = msg + ' CDC: ' + str(cdcval)
			else:
				msg = msg + ' CDC: Null'
		logging.debug('\t' + msg)
		return
