SQL Server Connector
====================

Overview
--------

The SQL Server Connector (referred to as Connector from here on out) is a successor to the [BG-BASE Connector](https://github.com/arnarb/BG-Connector). Connector is a library written in Python that uses the ESRI ArcGIS Python API (arcpy) and the ODBC Python API to synchronize data between the Arnold Arboretum's BG-BASE database and a geodatabase representation of the BG-BASE database.
The reason for the name change from BG-BASE Connector to SQL Server Connector is because, other than logging of BG-BASE specific data, the Python code has no knowledge of BG-BASE. Connector's sole purpose is to synchronize data in SQL Server CDC change tables and geodatabase datasets, and to generate XML change files for geodatabase-originated edits.

Code Structure
--------------

Connector has the following code structure:

* *config.py*: This top level file is a Python dictionary that configures the Connector.
* *sde_to_xml.py*: This top level file loads the Connector API to generate the XML change file for changes that originated in the geodatabase.
* *sqlserver_to_sde.py*: This top level file loads the Connector API to import changes that originated in BG-BASE into the geodatabase.
* *connector*: Package containing the implementation files of the Connector API.
	* *db.py*: File that contains Python classes that encapsulate database functionality.
		* *Replicas*: Python class that parses replicas from the config file.
		* *Replica*: Python class that encapsulates a replica. A replica contains an array of Datasets and manages the ODBC connection to the SQL Server.
		* *Dataset*: Python class that encapsulates a dataset. A dataset has a properties for a SQL Server table and a geodatabase dataset. The dataset class also contains functions that are used to read and parse data changes from the SQL Server CDC tables.
	* *io.py*: File that contains Python classes that encapsulate import and export functionality of the Connector.
		* *SqlServerImporter*: Python class that is called by the sqlserver_to_sde to import changes from the CDC tables into the geodatabase.
		* *GeodatabaseExporter*: Python class that is called by the sde_to_xml to generate an XML change file between geodatabase replicas.
* *util*: Package containing utility classes.
	* *DBUtil*: Python class that provides helper functions for ODBC objects.
	* *DateUtil*: Python class that provides helper functions for Date/Time objects.
	* *LockFile*: Python class that helps the Connector from running in multiple instances.