import arcpy
import json
import pyodbc
	
def get_count(dataset):
	return int(arcpy.GetCount_management(dataset).getOutput(0))

class Toolbox(object):
	def __init__(self):
		self.label = "Toolbox"
		self.alias = ""
		self.tools = [CreateFeatureClassFromTableTool]

class CreateFeatureClassFromTableTool(object):
	def __init__(self):
		self.label = "Create Feature Class From Table"
		self.description = "Creates a feature class or table in a geodatabase from external table"
		self.canRunInBackground = False

	def getParameterInfo(self):
		in_table = arcpy.Parameter(
			displayName="Input Table",
			name="in_table",
			datatype="DETable",
			parameterType="Required",
			direction="Input")
			
		in_workspace = arcpy.Parameter(
			displayName="Output Workspace",
			name="in_ws",
			datatype="DEWorkspace",
			parameterType="Required",
			direction="Input")
			
		in_name = arcpy.Parameter(
			displayName="Output Name",
			name="in_name",
			datatype="GPString",
			parameterType="Required",
			direction="Input")
			
		in_xfield = arcpy.Parameter(
			displayName="X Field",
			name="in_xfield",
			datatype="Field",
			parameterType="Optional",
			direction="Input")
		in_xfield.parameterDependencies = [in_table.name]
			
		in_yfield = arcpy.Parameter(
			displayName="Y Field",
			name="in_yfield",
			datatype="Field",
			parameterType="Optional",
			direction="Input")
		in_yfield.parameterDependencies = [in_table.name]
		
		in_sr = arcpy.Parameter(
			displayName="Spatial Reference",
			name="in_sr",
			datatype="GPSpatialReference",
			parameterType="Optional",
			direction="Input")
		in_sr.parameterDependencies = [in_xfield.name, in_yfield.name]
		in_sr.value = arcpy.SpatialReference(2249).exportToString()
			
		out_dataset = arcpy.Parameter(
			displayName="New Dataset",
			name="out_dataset",
			datatype="DEDatasetType",
			parameterType="Derived",
			direction="Output")
			
		return [in_table, in_workspace, in_name, in_xfield, in_yfield, in_sr, out_dataset]

	def isLicensed(self):
		return True

	def updateParameters(self, parameters):
		return

	def updateMessages(self, parameters):
		return
		
	def execute(self, parameters, messages):
		in_table = parameters[0].valueAsText
		in_ws = parameters[1].valueAsText
		in_name = parameters[2].valueAsText
		in_x = parameters[3].valueAsText
		in_y = parameters[4].valueAsText
		in_sr = parameters[5].valueAsText
		
		ws_desc = arcpy.Describe(in_ws)
		dataset = OdbcDataset(in_table)
		n = dataset.create(in_ws, in_name, in_x, in_y, in_sr)
		if n < 0:
			if dataset.createdDataset == False:
				arcpy.AddMessage("Failed to create output table")
			elif dataset.addedFields == False:
				arcpy.AddMessage("Failed to add fields to output table")
			else:
				arcpy.AddMessage("Failed to import all records.")
		else:
			arcpy.AddMessage("Adding GlobalID")
			try:
				arcpy.AddGlobalIDs_management([dataset.outputDataset])
			except Exception as e:
				arcpy.AddMessage("Error adding Global ID: " + e.message)
		
			if ws_desc.workspaceType == 'RemoteDatabase':
				arcpy.AddMessage("Registering as versioned")
				try:
					arcpy.RegisterAsVersioned_management(dataset.outputDataset, "NO_EDITS_TO_BASE")
				except Exception as e:
					arcpy.AddMessage("Error registering as versioned: " + e.message)
			parameters[6].value = dataset.outputDataset
		return
		

#Helper class to create GDB Dataset from SQL Server table		
class OdbcDataset(object):
	def __init__(self, in_ds):
		self.dataset = in_ds
		self.name = get_dataset_name(self.dataset)
		self.createdDataset = False
		self.addedFields = False;
		self.outputDataset = "";
		ws = get_workspace(in_ds)
		ws_desc = arcpy.Describe(ws)
		
		server = ws_desc.connectionProperties.server
		database = ws_desc.connectionProperties.database
		connectionString = "DRIVER={SQL Server};SERVER=${server};DATABASE=${database};Trusted_Connection=yes".replace('${server}', server).replace('${database}', database)
		self.connection = pyodbc.connect(connectionString)
		return
		
	def __del__(self):
		if self.connection is not None:
			try:
				self.connection.close()
			except:
				None
		return
		
	def create(self, destination, outputName, x_field, y_field, sr):
		dataset = destination + "\\" + outputName
		self.outputDataset = dataset
		num_records = -1
		try:
			if x_field is None or x_field == "" or y_field is None or y_field == "":
				arcpy.AddMessage("Creating table: " + outputName)
				arcpy.CreateTable_management(destination, outputName)
			else:
				#Only supporting points for now
				arcpy.AddMessage("Creating feature class: " + outputName)
				arcpy.CreateFeatureclass_management (destination, outputName, "POINT", None, None, None, sr)
			self.createdDataset = True
		except Exception as e:
			arcpy.AddMessage("Error creating dataset: " + e.message)
			return num_records
		
		field = []
		try:
			arcpy.AddMessage("Adding fields to " + outputName)
			fields = self._addFields(dataset)
			self.addedFields = True
		except Exception as e:
			arcpy.AddMessage("Error adding fields: " + e.message)
			return num_records
		
		try:
			arcpy.AddMessage("Loading data into " + outputName)
			num_records = self._loadRows(dataset, fields, x_field, y_field)
		except Exception as e:
			arcpy.AddMessage("Error loading rows: " + e.message)
			return num_records
		return num_records
		
	def _addFields(self, dataset):
		cursor = self.connection.cursor()
		fields = []
		for column in cursor.columns(table=self.name):
			fields.append(self._addField(dataset, column))
		return fields
		
	def _addField(self, dataset, column):
		field_name = column.column_name
		field_type = ""
		field_precision = None
		field_scale = None
		field_length = None
		field_is_nullable = "NON_NULLABLE"
		
		if column.nullable == 1:
			field_is_nullable = "NULLABLE"
			
		#Bug in SQL Server, refer to http://support.esri.com/en/bugs/nimbus/TklNMDk1NjQ4
		# Fixed in SP 10.3 10.2.2
		#Allow nulls for now
		field_is_nullable = "NULLABLE"
		
		n = column.type_name
		if n == 'varchar' or n == 'char':
			field_type = "TEXT"
			field_length = column.column_size
		elif n == 'numeric':
			field_type = "DOUBLE"
			field_precision = column.column_size
			field_scale = column.decimal_digits
		elif n == 'int' or n == 'int identity':
			field_type = "LONG"
			field_precision = column.column_size
		elif n == 'datetime':
			field_type = "DATE"
		
		arcpy.AddField_management (dataset, field_name, field_type, field_precision, field_scale, field_length, None, field_is_nullable)
		return field_name
		
	def _loadRows(self, dataset, fields, x_field, y_field):
		arcpy.AddMessage("inside _loadRows")
		insert_cursor = arcpy.InsertCursor(dataset);
		arcpy.AddMessage("Creating cursor")
		cursor = self.connection.cursor()
		cursor.execute("SELECT * from " + self.name)
		
		new_row = None
		n = 0
		field_name = ""
		try:
			for row in cursor:
				n = n + 1
				if n%1000 == 0:
					arcpy.AddMessage("Added " + str(n) + " records")
				x = None
				y = None
				new_row = insert_cursor.newRow();
				for i in xrange(len(fields)):
					field_name = fields[i];
					field_value = row[i];
					if field_value is not None:
						if field_value.__class__.__name__ == "Decimal":
							field_value = float(field_value)
						if field_name == x_field:
							x = field_value
						elif field_name == y_field:
							y = field_value
						new_row.setValue(field_name, field_value);
				if x is not None and y is not None:
					new_row.shape = arcpy.PointGeometry(arcpy.Point(x, y))
				insert_cursor.insertRow(new_row);
			arcpy.AddMessage("Added " + str(n) + " records to " + self.name)
		except Exception as e:
			arcpy.AddMessage("Error adding row: " + e.message)
			arcpy.AddMessage("Record: " + str(n) + ", Field: " + field_name)
			n = -1
		
		cursor.close()
		del new_row
		del insert_cursor
		return n
	
	def printDebug(self):
		arcpy.AddMessage("Iterating over " + get_dataset_name(self.dataset))
		cursor = self.connection.cursor()
		for row in cursor.columns(table=self.name):
			arcpy.AddMessage("Column: " + row.column_name)
			arcpy.AddMessage("Nullable: " + str(row.nullable == 1))
			arcpy.AddMessage("Data type: " + str(row.data_type))
			arcpy.AddMessage("Type name: " + row.type_name)
			arcpy.AddMessage("Size: " + str(row.column_size))
			arcpy.AddMessage("Precision: " + str(row.decimal_digits))
			arcpy.AddMessage(" ")

##########################
# String utility functions
##########################

def get_dataset_name(path):
	name = right(path, "\\");
	name = right(name, ".");
	return name;
	
def get_full_dataset_name(dataset):
	name = right(dataset, "\\");
	return name;
	
def get_workspace(path):
	end = last_index_of(path, "\\");
	if(end > -1):
		return substring(path, 0, end);
	else:
		return "";

def contains(s, c):
	index = s.find(c);
	if(index < 0):
		return False;
	else:
		return True;

def left(s, c):
	index = s.find(c);
	if(index < 0):
		return s;
	else:
		return s[:index];
		
def left_from_last(s, c):
	index = s.rfind(c);
	if(index < 0):
		return s;
	else:
		return s[0:index];

def right(s, c):
	index = s.rfind(c);
	if(index < 0):
		return s;
	else:
		return s[(index + 1):];
		
def last_index_of(s, c):
	return s.rfind(c);
		
def substring(s, start, end):
	return s[start:end];
	
def starts_with(s, c):
	return s.find(c) == 0;
	
def ends_with(s, c, last_char_only = True):
	if last_char_only:
		last_char = s[len(s) - 1:];
		if c == last_char:
			return True;
		else:
			return False;
	else:
		return s.rfind(c) > -1;
