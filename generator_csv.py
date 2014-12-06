import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import MySQLdb
import datetime
import time
import sys
import cStringIO
import zipfile
from ftplib import FTP, error_perm, error_reply, all_errors
	
#get yesterday's 15min data for a station for CSV maker
#
#	@param:		aws_id
#	@return:	csv file string
def get_15min_data(aws_id):
	'''colums: 
		DfW ID
		Date	
		Time	
		Ave AirTemp (AWS) (degC)	
		Ave AppTemp (degC)	
		Ave DewPoint (degC)	
		Ave Humidity (AWS) (%)	
		Ave DeltaT (degC)	
		Ave Soil Temperature (degC)	
		Ave GSR (W/m^2)	
		Min WndSpd (m/s)	
		Ave WndSpd (m/s)	
		Max WndSpd (m/s)	
		Ave WndDir (deg)	
		Total Rain (mm)	
		Ave LeafWet (% Wet)	
		Ave AirTemp (Canopy) (degC)	
		Ave Humidity (Canopy) (%)
	'''	
	
	sql = "SELECT COALESCE(tbl_stations.dfw_id,tbl_stations.aws_id), tbl_15min.* FROM tbl_15min INNER JOIN tbl_stations ON tbl_15min.aws_id = tbl_stations.aws_id WHERE tbl_15min.aws_id = '" + aws_id + "' AND DATE(stamp) = CURDATE() - INTERVAL 1 DAY GROUP BY stamp ORDER BY stamp;"
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])

	#cursor = conn.cursor (MySQLdb.cursors.DictCursor) -- for named columns
	cursor = conn.cursor()
	cursor.connection.autocommit(True)
	cursor.execute (sql)
	rows = cursor.fetchall()
	
	csv_string = ""
	
	for row in rows:
		date_time = str(row[3])
		row2 = []
		
		#turn None into 0
		for col in row:
			if col == None:
				row2.append(",")
			else:
				row2.append(str(col)+",")
		
		row_string = str(row2[0])+date_time[0:10]+","+date_time[11:]+","+str(row2[4])+str(row2[5])+str(row2[6])+str(row2[7])+str(row2[8])+str(row2[9])+str(row2[10])+str(row2[11])+str(row2[12])+str(row2[13])+str(row2[14])+str(row2[15])+str(row2[16])+str(row2[17])+str(row2[18])
		csv_string += row_string.strip(',') +"\r\n"
	
	cursor.close()
	conn.commit()
	conn.close()	
	
	return csv_string

#writes the 15min data for each station with status 'on' for DfW to a single CSV file (SAMDBNRM_YYYYMMDD.CSV)
#
#	@param:		owner
#	@return:	a single csv file string - with all the owner's stations' data from yesterday	
#	@calls:		get_15min_data
def make_csv_file(owner):
	sql = "SELECT aws_id FROM tbl_stations WHERE owner = '" + owner + "' AND status = 'on';"
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit (1)

	t = datetime.datetime.now()
	
	cursor = conn.cursor()
	cursor.connection.autocommit(True)
	cursor.execute(sql)
	rows = cursor.fetchall()

	single_csv_file = "DfW ID,Date,Time,Ave AirTemp (AWS) (degC),Ave AppTemp (degC),Ave DewPoint (degC),Ave Humidity (AWS) (%),Ave DeltaT (degC),Ave Soil Temperature (degC),Ave GSR (W/m^2),Min WndSpd (m/s),Ave WndSpd (m/s),Max WndSpd (m/s),Ave WndDir (deg),Total Rain (mm),Ave LeafWet (% Wet),Ave AirTemp (Canopy) (degC),Ave Humidity (Canopy) (%)\r\n"

	for row in rows:
		single_csv_file += get_15min_data(row[0])
	
	cursor.close()
	conn.close()	
	
	return single_csv_file

#send a single CSV file to the DfW
def send_csv_to_dfw(owner):
	#'''
	svr = 'e-nrims.dwlbc.sa.gov.au'
	usr = 'MEATelem'
	pwd = 'meatelem01'
	'''
	svr = 'kurrawong.net'
	usr = 'wdtf'
	pwd = 'wdtfwdtf'
	'''
	t = datetime.datetime.now()
	try:
		ftp = FTP(svr)
		ftp.set_debuglevel(0)
		ftp.login(usr, pwd)
		single_csv_file = cStringIO.StringIO(make_csv_file(owner))
		ftp.storbinary("STOR " + owner + "_" + t.strftime("%Y%m%d") + ".csv",single_csv_file)
		single_csv_file.close()
		ftp.quit()
	except error_reply:
		f = open("scheduled_exporter.log",'w')		
		f.write(t.strftime("%Y-%m-%d %H%i%s") + " ERROR for send_csv_to_dfw: " + str(error_reply))
		f.close()
		
#expected call: 

#param: stations' owner

#send_csv_to_dfw(owner)
#--> make_csv_file()
#----> get_15min_data()

#return: nothing
send_csv_to_dfw(sys.argv[1])
