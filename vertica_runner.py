#SQL RUNNER FOUNDATIONAL PROGRAM FOR VERTICA DATABASES

username = input("? USERNAME:") 
password = input("? PASSWORD:")
host = input("? HOST:")
port = input("? PORT:")
database_name = input("? DATABASE NAME:")

#def vertica_query(username,password,host,port,database_name)

	import pandas as pd #for data
	import pandas.io.sql as psql #for SQL
	import pypyodbc #for vertica / video data
	import vertica_python #for vertica / video data
	import csv
	import os

	print ("vertica_runner launched...\n \
		ABOUT: Created to run a simple query aggainst a vertica database using vertica_python\n\
		VERSION 1.0\n \
		CREATED 10.24.2018\n \
		BUILT BY TGURGICK\n \
		PYTHON CODE BASE\n \
		PACKAGES IMPORTED: pandas, pypyodbc, vertica_python, csv, os\n\n\n")
	print ("Select a database connection:\n" \
		"ENTER: 'vertica' ---- For vertica\n" \
		"ENTER: 'end'\n"
		)

	save_option = 'N'
	database = input("database:")

	###### connection details

	if database == 'vertica':
		connection_info = {
		    'host': host,
		             'port': port,
		             'user': username,
		             'password': password, 
		             'database': database_name,
		             # 10 minutes timeout on queries
		             'read_timeout': 600,
		             # default throw error on invalid UTF-8 results
		             'unicode_error': 'strict',
		             # SSL is disabled by default
		             'ssl': False,
		             # 'connection_timeout': 5
		             # connection timeout is not enabled by default
		}

		connection = vertica_python.connect(**connection_info)
		connection_cursor = connection.cursor()

		print ("connection to vertica established\n \
			VERTICA OPTIONS:\n \
			ENTER: 'custom' to run a query\n \
			ENTER: 'end' to end program")

		option = input("? OPTION:")

		if option == 'custom':
			query = input("? QUERY:")
			#columns = input("columns:")

			connection_cursor.execute(query)

			data = connection_cursor.fetchall() #turns it into dataset
			data = pd.DataFrame(data) #turns data into dataframe
			#data.columns = columns

			print (data)

			save_option = input("? SAVE(Y/N)?:")

		else:
			option = 'end'
			pass

	else:
		option = 'end'
		save_option = 'N'
		pass

	if save_option == "Y":
		try:
			cwd = os.getcwd()
			print("Current directory: %s" %(cwd))
			name = input("What do you want to call this file? ")
			file_path = '%s/%s.csv' %(cwd,name)
			data = pd.DataFrame(data,dtype=None)
			data.to_csv(file_path)	
			print("SAVED: %s" %(file_path))		
		except:
			pass
	elif save_option == "N":
		option = 'end'
	else:
		option = 'end'
		pass

	if database == 'end':
		option = 'end'

	if option == 'end':
		print ("Runner closed.")

#if __name__ == '__main__':
	#vertica_query(username,password,host,port,database_name)

