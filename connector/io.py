import os, sys, arcpy
import traceback, logging, uuid
import arcpy
import util
from time import strftime

###################################################################################################
###################################################################################################
#
# class:	SqlServerImporter
# purpose:	Worker class that reads data from the SQL Server CDC records and updates SDE.
#			Refreshes the data in Staging SDE Edit Version from the SQL Server Database.
#			Reconciles the data in the Staging Edit Version to Staging SDE Default Version
#			Synchronizes the data from Staging SDE Default Version to Production SDE Default Version.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
###################################################################################################


class SqlServerImporter(object):

	#replicas:	A db.Replicas object.
	def __init__(self, replicas):
		self._replicas = replicas
		self._dbutil = util.DBUtil()
		
	def run(self):
		func = 'SqlServerImporter.run'
		logging.info(" ")
		logging.info(" ")
		logging.info("******************************************************************************")
		logging.info("Begin " + func)
		
		for replica in self._replicas.replicas:
			self.processReplica(replica)
			
		logging.info("End " + func)
		logging.info("******************************************************************************")
		return
			
	def processReplica(self, replica):
		func = 'SqlServerImporter.processReplica'
		logging.info("Begin " + func)
		logging.info("Processing replica " + replica.name)
			
		lockfile = util.LockFile(replica.lockFilePath)
		if lockfile.locked():
			logging.error(replica.name + " is already running")
			logging.info('If %s is not running, then delete the file %s', replica.name, replica.lockFilePath)
			logging.info("End " + func)
			return
		lockfile.lock()
		
		num_changes = 0
		replica.connect()
		for dataset in replica.datasets:
			logging.debug('Processing dataset in ' + dataset.sdeTable)
			changes = self._importChanges(dataset)
			if changes > 0:
				num_changes = num_changes + changes
		replica.closeConnection()
			
		if num_changes < 1:
			lockfile.unlock()
			if num_changes == 0:
				logging.info('There are no changes from SQL Server. SDE sync will not run')
			else:
				logging.info('Failed to refresh staging from SQL Server, SDE sync will not run')
			logging.info("End " + func)
			return
			
		if replica.autoReconcile == True and self._reconcileStaging(replica) == False:
			lockfile.unlock()
			logging.info('Failed to reconcile data in staging between versions. SDE sync will not run')
			logging.info("End " + func)
			logging.info("******************************************************************************")
			return
			
		if self._syncWithProd(replica) == False:
			lockfile.unlock()
			logging.info('Failed to sync data between staging to production. SDE sync will not run')
			logging.info("End " + func)
			logging.info("******************************************************************************")
			return
			
		lockfile.unlock()
		logging.info("End " + func)
		return
		
	def _importChanges(self, dataset):
		func = 'SqlServerImporter._importChanges'
		logging.info('Begin ' + func)
		bImport = False
		processedRecords = []
		num_total = 0
		try:
			num_updates = 0
			num_updates_total = 0
			num_inserts = 0
			num_inserts_total = 0
			num_deletes = 0
			num_deletes_total = 0
			num_records = 0
			
			cursor = dataset.getChanges()
			fields = dataset.getChangeFields()
			if cursor is not None:
				logging.info("Begin iterating through change records")
				for row in cursor:
					operation = dataset.getOperationType(row)
					bProcessed = False
					if operation == "insert":
						num_inserts_total = num_inserts_total + 1
						if self._processInserts(dataset, row, fields) == True:
							num_inserts = num_inserts + 1
							bProcessed = True
					elif operation == "update":
						num_updates_total = num_updates_total + 1
						if self._processUpdates(dataset, row, fields) == True:
							num_updates = num_updates + 1
							bProcessed = True
					elif operation == "delete":
						num_deletes_total = num_deletes_total + 1
						if self._processDeletes(dataset, row, fields) == True:
							num_deletes = num_deletes + 1
							bProcessed = True
					else:
						continue
						
					if bProcessed:
						processedRecords.append(row[fields['__$CDCKEY']])
					
					num_records = num_records + 1
				cursor.close()
				del cursor
				cursor = None
				
				num_total = num_inserts + num_updates + num_deletes
				logging.info("Processed " + str(num_total) + " out of " + str(num_records) + " database operations")
				logging.debug('Number of inserts: ' + str(num_inserts) + ' out of ' + str(num_inserts_total))
				logging.debug('Number of updates: ' + str(num_updates) + ' out of ' + str(num_updates_total))
				logging.debug('Number of deletes: ' + str(num_deletes) + ' out of ' + str(num_deletes_total))
				
				logging.info("End iterating through change records")
		except:
			num_total = -1
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg)
		finally:
			if num_total > 0:
				dataset.clearChanges(processedRecords)
			logging.info('End ' + func)
		return num_total
			
	def _processInserts(self, dataset, row, fields):
		func = 'SqlServerImporter._processInserts'
		logging.info('Begin ' + func)
		features = None
		feature = None
		bInsert = False
		try:
			key = row[fields[dataset.cdcPrimaryKey]]
			layer = dataset.makeLayer(key)
			num_records = int(arcpy.GetCount_management(layer).getOutput(0))
			if num_records > 0:
				logging.error('Cannot insert record ' + str(key) + '. Record already exists')
				bInsert = True
			else:
				features = arcpy.InsertCursor(layer)
				field_names = self._getFieldNames(layer)
				dataset.logBgBaseInfo(None, row)
				
				feature = features.newRow()
				if self._loadFeature(feature, row, dataset, field_names, fields) == True:
					features.insertRow(feature)
					logging.debug('Successfully inserted record ' + str(key))
					bInsert = True
				else:
					logging.error("Insert failed for " + str(key) + ", could not load data for feature")
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		logging.debug('End ' + func)
		return bInsert

	def _processUpdates(self, dataset, row, fields):
		func = 'SqlServerImporter._processUpdates'
		logging.info('Begin ' + func)
		features = None
		feature = None
		bUpdate = False
		try:
			key = row[fields[dataset.cdcPrimaryKey]]
			layer = dataset.makeLayer(key)
			features = arcpy.UpdateCursor(layer)
			field_names = self._getFieldNames(layer)
			
			num_features = 0
			for feature in features:
				num_features = num_features + 1
				dataset.logBgBaseInfo(feature, row)
				
				if self._loadFeature(feature, row, dataset, field_names, fields) == True:
					features.updateRow(feature)
					logging.debug('Successfully updated record ' + str(key))
					bUpdate = True
				else:
					logging.error("Failed to load feature")
			
			if num_features == 0:
				logging.warn('Update cursor contained no features for ' + dataset.sdePrimaryKey + ' = ' + str(key))
				logging.warn('Attempting to insert ' + str(key) + ' instead')
				if feature:
					del feature
					feature = None
				if features:
					del features
					features = None
				bUpdate = self._processInserts(dataset, row, fields)
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		logging.debug('End ' + func)
		return bUpdate
		
	def _processDeletes(self, dataset, row, fields):
		func = 'SqlServerImporter._processDeletes'
		logging.info('Begin ' + func)
		features = None
		feature = None
		bDelete= False
		try:
			key = row[fields[dataset.cdcPrimaryKey]]
			layer = dataset.makeLayer(key)
			features = arcpy.UpdateCursor(layer)
			
			num_features = 0
			for feature in features:
				features.deleteRow(feature)
				num_features = num_features + 1
				logging.debug('Successfully deleted record ' + str(key))
				
			bDelete = num_features > 0
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		logging.debug('End ' + func)
		return bDelete
		
	def _getFieldNames(self, feature_class):
		fields = arcpy.ListFields(feature_class)
		names = []
		for field in fields:
			if field.type == "OID" or field.type == "Geometry":
				continue
			names.append(field.name)
		return names
	
	def _loadFeature(self, feature, row, dataset, feature_fields, row_fields):
		func = 'SqlServerImporter._loadFeature'
		last_field = ''
		last_value = ''
		try:
			for field_name in feature_fields:
				if row_fields.has_key(field_name):
					new_value = row[row_fields[field_name]]
					last_field = field_name
					if new_value is not None:
						last_value = new_value
					else:
						last_value = 'None'
					try:
						if str(type(new_value)) == "<class 'decimal.Decimal'>":
							new_value = float(new_value)
						feature.setValue(field_name, new_value)
					except arcpy.ExecuteError:
						msgs = arcpy.GetMessages(0)
						arcpy.AddError(msgs)
						logging.error("ArcGIS error: %s", msgs)
						logging.error('Field/Value: %s, %s', last_field, last_value)
						logging.error(type(last_value))
					except:
						tb = sys.exc_info()[2]
						tbinfo = traceback.format_tb(tb)[0]
						msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
						arcpy.AddError(msg)
						logging.error(msg)
						logging.error('Field/Value: %s, %s', last_field, last_value)
						logging.error(type(last_value))
				elif field_name != "GlobalID":
					logging.warn(field_name + " not found in Warehouse")

			if dataset.isSpatial == True and row_fields.has_key(dataset.xField) and row_fields.has_key(dataset.yField):
				x = row[row_fields[dataset.xField]]
				y = row[row_fields[dataset.yField]]
				if x is not None and y is not None:
					if str(type(x)) == "<class 'decimal.Decimal'>":
						x = float(x)
					if str(type(y)) == "<class 'decimal.Decimal'>":
						y = float(y)
					feature.shape = arcpy.PointGeometry(arcpy.Point(x, y))
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
			logging.error('Field/Value: %s, %s', last_field, last_value)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
			logging.error('Field/Value: %s, %s', last_field, last_value)
		return False
		
	def _reconcileStaging(self, replica):
		func = 'SqlServerImporter.reconcile_staging'
		logging.info("Begin " + func)
		try:
			logging.debug("Reconciling data in " + replica.stagingWorkspace + ", from " + replica.sqlserverEditVersion + " to " + replica.stagingDefaultVersion)
			arcpy.ReconcileVersions_management(replica.stagingWorkspace, "ALL_VERSIONS", replica.stagingDefaultVersion, replica.sqlserverEditVersion, "NO_LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "POST", "KEEP_VERSION")
			logging.debug("Finished reconciling data.")
			
			logging.debug("Compressing data in Staging SDE")
			arcpy.Compress_management(replica.stagingWorkspace)
			logging.debug("Finished compressing data in Staging SDE")
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return False
		
	def _syncWithProd(self, replica):
		func = 'SqlServerImporter._syncWithProd'
		logging.info("Begin " + func)
		try:
			logging.debug("Synchronizing data from staging to production")
			arcpy.SynchronizeChanges_management(replica.stagingWorkspace, replica.name, replica.productionWorkspace, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
			logging.debug("Finished synchronizing data from production to staging")
			
			logging.debug("Compressing data in Production SDE")
			arcpy.Compress_management(replica.productionWorkspace)
			logging.debug("Finished compressing data in Production SDE")
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return False
		
###################################################################################################
###################################################################################################
#
# class:	GeodatabaseExporter
# purpose:	Worker class iterates over the configured replicas and writes out change files
#			for changes that occurred between staging and production.
#
# author:	Jason Sardano
# date:		Sep 30, 2013
#
###################################################################################################


class GeodatabaseExporter(object):

	#replicas:	A db.Replicas object.
	def __init__(self, replicas):
		self._replicas = replicas
		self._dbutil = util.DBUtil()
		
	def run(self):
		func = 'GeodatabaseExporter.run'
		logging.info(" ")
		logging.info(" ")
		logging.info("******************************************************************************")
		logging.info("Begin " + func)
		
		for replica in self._replicas.replicas:
			self.processReplica(replica)
			
		logging.info("End " + func)
		logging.info("******************************************************************************")
		return
		
	def processReplica(self, replica):
		func = 'GeodatabaseExporter.processReplica'
		logging.info("Begin " + func)
		logging.info("Processing " + replica.name)
		
		ts = strftime("%m%d%Y_%H%M%S")
		tempFile = replica.tempPath + '\\temp_' + ts + '.xml'
		exportFile = replica.exportPath + '\\changes_' + ts + '.xml'
		
		if replica.autoReconcile == True:
			logging.info('Reconciling edits from edit versions to default in staging')
			self._reconcileStaging(replica)
			
		logging.info("Exporting XML change file for " + replica.name)
		if self._exportChangeFile(replica, tempFile) == False:
			msg = 'Failed to create XML change file. Make sure that you have sufficient permissions in ' + replica.tempPath
			arcpy.AddError(msg)
			logging.error(msg)
			logging.error("Export change file failed. Sync will not run.")
			logging.info("******************************************************************************")
			return
			
		logging.info("Synchronizing changes in Staging Default SDE with Production SDE")
		if self._syncWithProd(replica) == False:
			logging.error("Failed to sync with prod. Sync will not run.")
			logging.info("******************************************************************************")
			return False
			
		arcpy.AddMessage("Sending XML change file to BG-BASE folder queue")
		if self._sendChangeFile(replica, tempFile, exportFile) == False:
			msg = 'Failed to copy XML change file to folder queue. Make sure that you have sufficient permissions in ' + replica.exportPath
			logging.error(msg)
			arcpy.AddError(msg)
			
		logging.info("End " + func)
		return
		
	def _reconcileStaging(self, replica):
		func = 'GeodatabaseExporter._reconcileStaging'
		logging.info("Begin " + func)
		
		try:
			logging.debug('Getting edits versions')
			versions = replica.stagingEditVersions
			if len(versions) > 0:
				logging.debug('Found ' + str(len(versions)) + ' edit versions to reconcile.')
				logging.debug("Reconciling data with Staging DEFAULT")
				arcpy.ReconcileVersions_management(replica.stagingWorkspace, "ALL_VERSIONS", replica.stagingDefaultVersion, ";".join(versions), "NO_LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "POST", "KEEP_VERSION")
				logging.debug("Finished reconciling data with Staging DEFAULT")
			
			logging.debug("Compressing data in Staging SDE")
			arcpy.Compress_management(replica.stagingWorkspace)
			logging.debug("Finished compressing data in Staging SDE")
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return
		
	def _exportChangeFile(self, replica, tempFile):
		func = 'GeodatabaseExporter._exportChangeFile'
		result = False
		logging.info("Begin " + func)
		try:
			logging.debug("Exporting data change message to: %s", tempFile)
			arcpy.ExportDataChangeMessage_management(replica.stagingWorkspace, tempFile, replica.name, "DO_NOT_SWITCH", "UNACKNOWLEDGED", "NEW_CHANGES")
			result = True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
			
		logging.info("End export change file")
		return result
		
	def _syncWithProd(self, replica):
		func = 'GeodatabaseExporter._syncWithProd'
		logging.info("Begin " + func)
		try:
			logging.debug("Synchronizing data from staging to production")
			arcpy.SynchronizeChanges_management(replica.stagingWorkspace, replica.name, replica.productionWorkspace, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
			logging.debug("Finished synchronizing data from production to staging")
			
			logging.debug("Compressing data in Production SDE")
			arcpy.Compress_management(replica.productionWorkspace)
			logging.debug("Finished compressing data in Production SDE")
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return False
		
	def _sendChangeFile(self, replica, tempFile, exportFile):
		result = False
		func = 'GeodatabaseExporter._sendChangeFile'
		logging.info("Begin " + func)
		try:
			logging.debug('Copying %s to %s', tempFile, exportFile)
			self._copyFile(tempFile, exportFile)
			if replica.deleteTempFiles:
				self._deleteFile(tempFile)
			result = True
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End "  + func)
		return result
		
	def _deleteFile(self, path_to_file):
		try:
			os.remove(path_to_file)
		except:
			#do nothing
			None
		return
		
	def _copyFile(self, source, dest):
		result = False
		func = 'GeodatabaseExporter._copyFile'
		logging.info("Begin " + func)
		try:
			with open(source, 'r') as s:
				with open(dest, 'w') as d:
					line = s.readline()
					while line:
						d.write(line)
						line = s.readline()
			result = True
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return result
