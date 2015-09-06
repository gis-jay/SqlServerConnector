##Arnold Arboretum BGBASE - SQL Server Connector
###Overview
The Arnold Arboretum BG-BASE - SQL Server Connector, referred to as *Connector* from here on out, is a library that is written in Python to faciliate the movement of data
bewtween the Arboretum's BGBASE database and its ArcGIS SDE database. The Connector contains 2 files that are launched from the Python executable
to move the Arboretum's data: **_sqlserver_to_sde.py_** and **_sde_to_xml.py_**. The code is controlled by a configuration file, **_config.py_**, that indicates
where various data sources exist, as well as other settings, such as the location of log files.

Connector uses Python 2.7, the 32-bit Python ODBC client (pyodbc) to interact with the Warehouse database and the ArcGIS Python library (arcpy) to interact with the ArcGIS database.

###General System Architecture
The Connector is made up of multiple systems. Arnold Arboretum uses BG-BASE to maintain its plant information, and uses ArcGIS Server to make its data spatially available.
A non-spatial database sits in between the BG-BASE database and ArcGIS database that is critical in getting the changes from BG-BASE to ArcGIS. This non-spatial database
is a SQL Server database, and the database is configured to use Change Data Capture (CDC) to track changes.

BG-BASE and the Arnold Arboretum designed a relational database in SQL Server that is a representation of the BG-BASE data. This database is named **Warehouse**.
When changes are made in the BG-BASE database, the BG-BASE software will replicate these changes in the Warehouse database using an ODBC connection. The tables
that are updated in Warehouse must have CDC enabled in order for the changes to propagate to ArcGIS. When called using the **sqlserver_to_sde.py** code,
the Connector will read the changes from the CDC tables, and will apply the changes from the Warehouse database to the ArcGIS database.

The Connector requires an ArcGIS Geodatabase Replica in order to propagate changes from the spatial database to the BG-BASE database. The replica is a one-way
replica, with changes moving from the parent replica to the child replica. In the Arboretum's system, the parent replica is contained in a database named **Staging**,
and the child replica is contained in a database named **Production**. Users make edits in the Staging geodatabase, and then run the Connector's **sde_to_xml.py**
code that will generate an XML data change file and place the file in a location that is known to BG-BASE. BG-BASE will pick up the change file, and will update
the internal BG-BASE database.

*It is important to note that the Connector only supports updates that are made in the ArcGIS database. New records from ArcGIS will not be applied to BG-BASE.*