This project has been written in Eclipse Java IDE, with JRE 1.8 and higher version.
The project uses javafx, which has been added to the access rules of the project as follows:
Right click folder -> Properties -> Java Build Path -> Libraries (under JRE expand) -> Select Access Rules and Edit -> add under 'Accessible' value 'javafx/**'.
Go to the file named DavisBasePrompt.java under the src folder in DavisBaseFinal project folder and run to access the rudimentary database.
The following commands are supported:

USE DATABASE database_name;                      Changes current database.
CREATE DATABASE database_name;                   Creates an empty database.
SHOW DATABASES;                                  Displays all databases.
DROP DATABASE database_name;                     Deletes a database.
SHOW TABLES;                                     Displays all tables in current database.
CREATE TABLE table_name (                        Creates a table in current database.
	<column_name> <datatype> [PRIMARY KEY | NOT NULL]
	...);
DROP TABLE table_name;                           Deletes a table data and its schema.
SELECT <column_list> FROM table_name             Display records whose rowid is <id>.
	[WHERE rowid = <value>];
SELECT <column_list> FROM table_name		 Display records by attribute name.
INSERT INTO table_name                           Inserts a record into the table.
	[(<column1>, ...)] VALUES (<value1>, <value2>, ...);
HELP;                                            Displays help information
EXIT;                                            Exits the program

All the data for the database is saved in the data folder under DavisBaseFinal project folder as non-volatile .tbl files for each database.