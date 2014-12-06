#!/bin/python
'''
4a.	rain						mm	time
4b.	Instantaneous wind speed	ms	time
4c.	Class A  pan				mm	time
4d.	
	Global solar exposure
	reflected global solar exposure	
	downward longwave exposure
	upward longwave exposure
	net exposure
		a exposure expressed in joules per square metre
		b length of the exposure expressed in seconds
		c estimated 95% uncertainty expressed in joules per square metre
		d number of samples added
		e time and date the measurement finished
4e.	
	Global solar irradiance, 
	reflected global solar irradiance, 
	downward longwave irradiance, 
	upward longwave irradiance, 
	net irradiance
		a average irradiance expressed in watts per square metre
		b averaging period expressed in seconds
		c estimated 95% uncertainty expressed in watts per square metre
		d number of samples averaged
		e sample standard deviation expressed in watts per square metre
		f time and date the measurement finished
4f.	Instantaneous dry-bulb airT		deg C	time
4g.	Instantaneous wet-bulb airT		deg C	time
4h.	Instantaneous RH				%		time
4i.	Instantaneous VP				mbar	time
'''
import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import MySQLdb
import datetime
from datetime import date, timedelta
import time
import sys
import os
import zipfile
from ftplib import FTP, error_perm, error_reply, all_errors
import logging

#add each station's values to the wdtf_file
#
#	@param:		aws_id, member (DB column)
#	@return:	XML - observationMember
def get_observation_member(aws_id, member, wdtf_id, in_date):
	logging.debug("get_hydrocollection " + aws_id + " " + member + " " + wdtf_id + " " + in_date.strftime("%Y-%m-%d"))
	
	dat_str = in_date.strftime("%Y-%m-%d")
	if member == 'rain_total':#rainguages only
		sql = "SELECT CONCAT(DATE_FORMAT(stamp,'%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp, " + member + " FROM tbl_daily WHERE stamp BETWEEN '" + dat_str + " 00:00:00' AND '" + dat_str + " 11:59:00' AND aws_id = '" + aws_id + "' ORDER BY aws_id, stamp;"
	else:
		sql = "SELECT CONCAT(DATE_FORMAT(stamp - INTERVAL 9 HOUR - INTERVAL 30 MINUTE,'%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp," + member + " FROM tbl_15min WHERE stamp BETWEEN '" + dat_str + " 00:00:00' AND '" + dat_str + " 11:59:00' AND aws_id = '" + aws_id + "' ORDER BY aws_id, stamp;"

	logging.debug(sql)
	
	if member == 'rain_total':#rainguages only
		gml_id = "TS_rain"
		feature = 'Rainfall_mm'
		interpol = 'PrecTot'
		units = 'mm'	
	elif member == 'rain':
		gml_id = "TS_rain"
		feature = 'Rainfall_mm'
		interpol = 'InstTot'
		units = 'mm'		
	elif member == 'Wavg':
		gml_id = "TS_Wavg"
		feature = 'WindSpeed_ms'
		interpol = 'InstVal'
		units = 'm/s'
	elif member == 'gsr':
		gml_id = "TS_gsr"
		feature = 'GlobalSolarIrradianceAverage_Wm2'
		interpol = 'PrecVal'
		units = 'W/m2'
	elif member == 'airT':
		gml_id = "TS_airT"
		feature = 'DryAirTemperature_DegC'
		interpol = 'InstVal'
		units = 'Cel'
	elif member == 'rh':
		gml_id = "TS_rh"
		feature = 'RelativeHumidity_Perc'
		interpol = 'InstVal'
		units = '%'
	
	#elif member == 'dp':
	#	gml_id = "TS_dp"
	#	feature = 'DewPoint_DegC'
	#	interpol = 'InstVal'
	#	units = 'Cel'
	
		
	wdtf_obsMember = '''
	<wdtf:observationMember>
		<wdtf:TimeSeriesObservation gml:id="''' + gml_id + '''">
			<gml:description>Weatherstation data</gml:description>
			<gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/TimeSeriesObservation/''' + wdtf_id + '''/">1</gml:name>	
			<om:procedure xlink:href="urn:ogc:def:nil:OGC::unknown"/>
			<om:observedProperty xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/property//bom/''' + feature + '''"/>
			<om:featureOfInterest xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/feature/SamplingPoint/''' + wdtf_id + '''/''' + aws_id + '''/1"/>
			<wdtf:metadata>
				<wdtf:TimeSeriesObservationMetadata>
					<wdtf:relatedTransaction xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/definition/sync/bom/DataDefined"/>
					<wdtf:siteId>''' + aws_id + '''</wdtf:siteId>
					<wdtf:relativeLocationId>1</wdtf:relativeLocationId>
					<wdtf:relativeSensorId>''' + aws_id + '''_aws</wdtf:relativeSensorId>					
					<wdtf:status>validated</wdtf:status>
				</wdtf:TimeSeriesObservationMetadata>
			</wdtf:metadata>
			<wdtf:result>
				<wdtf:TimeSeries>
					<wdtf:defaultInterpolationType>''' + interpol + '''</wdtf:defaultInterpolationType>
					<wdtf:defaultUnitsOfMeasure>''' + units + '''</wdtf:defaultUnitsOfMeasure>
					<wdtf:defaultQuality>quality-A</wdtf:defaultQuality>
 '''
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])

	#cursor = conn.cursor (MySQLdb.cursors.DictCursor) -- for named columns
	cursor = conn.cursor()
	cursor.connection.autocommit(True)
	cursor.execute (sql)
	rows = cursor.fetchall()
	for row in rows:
		wdtf_obsMember += "					<wdtf:timeValuePair time=\"%s\">%s</wdtf:timeValuePair>\n" % (row[0], row[1])
	
	cursor.close()
	conn.commit()
	conn.close()
	
	wdtf_obsMember += '''		
				</wdtf:TimeSeries>
			</wdtf:result>
		</wdtf:TimeSeriesObservation>
	</wdtf:observationMember>'''

	return wdtf_obsMember
		
#get the full WDTF for a particular station
#
#	@param:		aws_id
#	@return:	XML - HydroCollection
#	@calls:		get_observation_member(aws_id, member, wdtf_id)
def get_hydrocollection(aws_id, wdtf_id, in_date):
	logging.debug("get_hydrocollection " + aws_id + " " + wdtf_id)
	
	t = datetime.datetime.utcnow()
	
	hydrocollection = '''<?xml version="1.0"?>
 <wdtf:HydroCollection
	xmlns:sa="http://www.opengis.net/sampling/1.0/sf1"
	xmlns:om="http://www.opengis.net/om/1.0/sf1"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:xlink="http://www.w3.org/1999/xlink"
	xmlns:gml="http://www.opengis.net/gml"
	xmlns:wdtf="http://www.bom.gov.au/std/water/xml/wdtf/1.0"
	xmlns:ahgf="http://www.bom.gov.au/std/water/xml/ahgf/0.2"
	xsi:schemaLocation="http://www.opengis.net/sampling/1.0/sf1 ../sampling/sampling.xsd 
	http://www.bom.gov.au/std/water/xml/wdtf/1.0 ../wdtf/water.xsd
	http://www.bom.gov.au/std/water/xml/ahgf/0.2 ../ahgf/waterFeatures.xsd"
	gml:id="timeseries_m">
	<gml:description>This document encodes timeseries data from the SANRM's Automatic Weatherstation Network.</gml:description>
	<gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/HydroCollection/''' + wdtf_id + '''/">wdtf_sanrm</gml:name>
	
	<wdtf:metadata>
		<wdtf:DocumentInfo>
			<wdtf:version>wdtf-package-v1.0</wdtf:version>
			<wdtf:dataOwner codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_id + '''</wdtf:dataOwner>
			<wdtf:dataProvider codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_id + '''</wdtf:dataProvider>
			<wdtf:generationDate>''' + t.strftime("%Y-%m-%dT%H:%M:%S") + '''+09:30</wdtf:generationDate>
			<wdtf:generationSystem>KurrawongIC_WDTF</wdtf:generationSystem>
		</wdtf:DocumentInfo>
	</wdtf:metadata>
 '''	
	#separate format for rainguages
	if aws_id[:4] == 'TBRG':
		#used to use daily total for TBRG, now using 15min data
		#hydrocollection += get_observation_member(aws_id, 'rain_total', wdtf_id, in_date)
		hydrocollection += get_observation_member(aws_id, 'rain', wdtf_id, in_date)
	else:#aws
		logging.debug("get_hydrocollection before get_observation_member for rain")
		hydrocollection += get_observation_member(aws_id, 'rain', wdtf_id, in_date)
		logging.debug("get_hydrocollection after get_observation_member for rain")
		hydrocollection += get_observation_member(aws_id, 'Wavg', wdtf_id, in_date)
		hydrocollection += get_observation_member(aws_id, 'gsr', wdtf_id, in_date)
		hydrocollection += get_observation_member(aws_id, 'airT', wdtf_id, in_date)
		hydrocollection += get_observation_member(aws_id, 'rh', wdtf_id, in_date)
		#hydrocollection += get_observation_member(aws_id, 'dp', wdtf_id, in_date)
	
	hydrocollection += "</wdtf:HydroCollection>"

	return hydrocollection

#make a WDTF XML file for each station for a particular owner with status 'on' and returns them as zip file
#
#	@param:		owner
#	@return:	string: zip file name
#	@calls:		get_hydrocollection(owner, wdtf_id)
def make_wdtf_zip_file(owner, in_date):
	logging.debug("make_wdtf_zip_file " + owner)
	
	sql = "SELECT aws_id, wdtf_id FROM tbl_stations INNER JOIN tbl_owners ON tbl_stations.owner = tbl_owners.owner_id WHERE `owner` = '" + owner + "' AND `status` = 'on';"
	
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
	wdtf_file_names = []
	wdtf_file_data = []
	wdtf_id = ''
	#make double array of file names & file data
	for row in rows:
		wdtf_id = row[1]
		#wdtf.w00208.20111225H0000.RMPW12-ctsd.xml
		wdtf_file_names.append("wdtf." + wdtf_id + "." + t.strftime("%Y%m%d%H0000") + "." + row[0] + "-ctsd.xml")
		wdtf_file_data.append(get_hydrocollection(row[0], row[1], in_date))
	
	cursor.close()
	conn.close()
	
	#make the zipfile from file names & file data
	#w00208.20111225093000.zip
	zfilename = wdtf_id + "." + in_date.strftime("%Y%m%d") + "093000.zip"#fixed at 9:30am
	logging.debug("zip file name " + zfilename)
	zout = zipfile.ZipFile(os.getcwd() + "/" + zfilename, "w", zipfile.ZIP_DEFLATED)
	
	for i in range(len(wdtf_file_names)):
		zout.writestr(wdtf_file_names[i],wdtf_file_data[i])
	zout.close()
	
	#we have created a zipfile on disk so return the file name
	return zfilename 

#make a WDTF XML file for each station for a particular owner with status 'on' and returns them as zip file
#
#	@param:		owner
#	@return:	string: zip file name
#	@calls:		get_hydrocollection(owner, wdtf_id)
def make_wdtf_zip_file_for_station_and_date(owner, aws_id, in_date):
	#logging.debug("make_wdtf_zip_file_for_station_and_date " + owner + " " + aws_id + " " + in_date.strftime("%Y%m%d%H0000"))
	logging.debug("make_wdtf_zip_file_for_station_and_date " + owner + " " + aws_id)
	#get the WDTF ID for this owner
	sql = "SELECT wdtf_id FROM tbl_owners WHERE `owner_id` = '" + owner + "';"
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit (1)
	
	#t = datetime.datetime.now()
	t = in_date
	
	cursor = conn.cursor()
	cursor.connection.autocommit(True)
	cursor.execute(sql)
	rows = cursor.fetchall()
	wdtf_file_names = []
	wdtf_file_data = []
	wdtf_id = ''
	#make double array of file names & file data
	for row in rows:
		wdtf_id = row[0]
		#wdtf.w00208.20111225H0000.RMPW12-ctsd.xml
		wdtf_file_names.append("wdtf." + wdtf_id + "." + t.strftime("%Y%m%d%H0000") + "." + aws_id + "-ctsd.xml")
		#get_hydrocollection(aws_id, wdtf_id)
		wdtf_file_data.append(get_hydrocollection(aws_id, row[0], in_date))
	
	cursor.close()
	conn.close()
	
	#make the zipfile from file names & file data
	#w00208.20111225093000.zip
	zfilename = wdtf_id + "." + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".zip"#now
	zout = zipfile.ZipFile(os.getcwd() + "/" + zfilename, "w", zipfile.ZIP_DEFLATED)
	
	for i in range(len(wdtf_file_names)):
		zout.writestr(wdtf_file_names[i],wdtf_file_data[i])
	zout.close()
	
	#we have created a zipfile on disk so return the file name
	return zfilename
	
#send the zipped WDTF file collection to the BoM by FTP using owner's FTP WDTF details
#	@param:		owner
#	@return:	nothing
#	@calls:		make_wdtf_zip_file(owner)
def send_wdtf_zipfile(owner, in_date):
	logging.debug("send_wdtf_zipfile " + owner)
	#get the owner's WDTF details
	sql = "SELECT wdtf_server,wdtf_id,wdtf_password FROM tbl_owners WHERE owner_id = '" + owner + "';"
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit (1)

	cursor = conn.cursor()
	cursor.connection.autocommit(True)
	cursor.execute (sql)
	rows = cursor.fetchall()
	
	svr = ''
	usr = ''
	pwd = ''
		
	for row in rows:
		svr = row[0]
		usr = row[1]
		pwd = row[2]
	
	cursor.close()
	conn.close()
	
	#dummy FPT details for testing
	'''
	svr = 'kurrawong.net'
	usr = 'wdtf'
	pwd = 'wdtfwdtf'
	'''
	
	#get the zip file
	zipfile = make_wdtf_zip_file(owner, in_date)
	
	#send the zip file
	try:
		ftp = FTP(svr)
		ftp.set_debuglevel(0)
		ftp.login(usr, pwd)
		ftp.cwd('/register/' + usr + '/incoming/data')
		ftp.storbinary("STOR " + zipfile,open(zipfile,'rb') )
		ftp.quit()
	except error_reply:
		t = datetime.datetime.now()
		f = open("scheduled_export.log",'w')		
		f.write(t.strftime("%Y-%m-%d %H%i%s") + " ERROR for send_wdtf_zipfile: " + str(error_reply))
		f.close()
	
	#delete zip file on disc
	os.remove(zipfile)
	
if __name__ == "__main__":
	logging.basicConfig(filename="/home/ftp/wdtf/wdtf_generator.log",level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S',format='%(asctime)s %(levelname)s %(message)s')
	#expected call: 
	#param: stations' owner
	#methods:
	#send_wdtf_zipfile(owner)
	#--> make_wdtf_zipfile()
	#----> get_hydrocollection()
	#------> get_observation_member()
	#return: zipfile

	## manual, make zip file for one station
	# python /home/ftp/wdtf/generator_wdtf.py station SAMDB RMPW12 2013-08-27
	if sys.argv[1] == "station":
		owner = sys.argv[2]
		aws_id = sys.argv[3]
		in_date = datetime.datetime.strptime(sys.argv[4], "%Y-%m-%d")
		
		zipfile = make_wdtf_zip_file_for_station_and_date(owner, aws_id, in_date)
		logging.info("manual station " + owner + " : " + aws_id + " : " + sys.argv[4] + " : file " + zipfile)
	elif sys.argv[1] == "owner":
		# python /home/ftp/wdtf/generator_wdtf.py owner SAMDB 2013-08-27
		owner = sys.argv[2]
		in_date = datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d")
		zipfile = make_wdtf_zip_file(owner, in_date)
		logging.info("manual owner " + owner + " : " + sys.argv[3] + " : file " + zipfile)
	else:	
		#automated, daily run from crontab
		# python /home/ftp/wdtf/generator_wdtf.py SAMDB
		# python /home/ftp/wdtf/generator_wdtf.py SENRM
		# python /home/ftp/wdtf/generator_wdtf.py LMW
		# python /home/ftp/wdtf/generator_wdtf.py AWNRM
		owner = sys.argv[1]
		in_date = (date.today() - timedelta(1))
		logging.info("cron " + owner + " : " + in_date.strftime('%Y-%m-%d'))
		send_wdtf_zipfile(owner, in_date)
