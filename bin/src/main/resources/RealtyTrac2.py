#!/usr/bin/python

import sys
import httplib2 # https://code.google.com/p/httplib2/wiki/Examples
#import urllib2 # Fail frequently
import csv
from time import sleep
import random
import datetime
#import HTMLParser
#import html2text
#import shlex #shlex.split(myStr)
#

# ================= Utility Functions  ================= 

# This function return
def Flip_TF(s):
	if s:
		return(False);
	else:
		return(True);
# End of Flip_TF

def html_parser(a):
	# Init
	iStart=0; iEnd=0;
	inMarkup=False;
	sq=False; # single quotation
	dq=False; # double quotation
	split=False;
	x=[];
	while iEnd<len(a):
		if a[iEnd] == '<' :
			inMarkup=True;
			split=True;
		elif a[iEnd] == '>' :
			inMarkup=False;
			split=True;
		elif a[iEnd] == "'" :
			sq=Flip_TF(sq);
		elif a[iEnd] == '"' :
			dq=Flip_TF(dq);
		elif a[iEnd] == ' ' and inMarkup and (not (sq or dq)) :
			split=True;
		if split :
			if(iEnd>iStart):
				s=a[iStart:iEnd]
				if len(x)==0 :
					x=[s];
				else:
					x.append(s);
			if (iEnd+1)< len(a) and a[iEnd+1] in ['<'] :
				inMarkup=True;
				iStart=iEnd+2;
			else:
				iStart=iEnd+1;
			iEnd=iStart+1;
			split=False;
		else:
			iEnd=iEnd+1
	# Last one
	if(iEnd>iStart):
		s=a[iStart:iEnd]
		if len(x)==0 :
			x=[s];
		else:
			x.append(s);
	return(x)
# End of html_parser()

# This function fetches addresses of a given URL.
def get_addresses(urlAddress, saleStatus, county_name):
	tryCount=0;
	while tryCount < 10:
		try:
			#h = httplib2.Http(".cache");
			h = httplib2.Http();
			resp, html = h.request(urlAddress, "GET")
			break;
		except:
			tryCount=tryCount+1;
			print 'tryCount='+str(tryCount);
			del h;
			sleep(10.0);
	#response = urllib2.urlopen(urlAddress)
	#html = response.read()
	a0=html_parser(html);
	# Init
	nAddresses=0; i=0; j=0; maxPage=0; addresses=[]; address=[''];
	na0=len(a0);
	# Process the data.
	while i < na0 :
		if a0[i]=='class="propertyLink"' : # beginning of new address
			nAddresses=nAddresses+1;
		elif nAddresses > 0 and a0[i]=="itemprop='streetAddress'" :
			i=i+1;
			s=str(a0[i]);
			s=s.replace(',','');
			address=[s];
		elif nAddresses > 0 and a0[i]=="itemprop='addressLocality'" : # City
			i=i+1;		
			address.append(str(a0[i]));
			address.append(county_name); # Append the county name after the city name.
		elif nAddresses > 0 and a0[i]=="itemprop='addressRegion'" : # State
			i=i+1;
			address.append(str(a0[i]));
		elif nAddresses > 0 and a0[i]=="itemprop='postalCode'" : # Zip Code
			i=i+1;
			address.append(str(a0[i]));
			address.append(str(saleStatus)); # Append the sales status after the ZIP code.
		elif nAddresses > 0 and a0[i]=='itemprop="datePublished"' :
			i=i+1;
			address.append(str(a0[i]));
		elif nAddresses > 0 and a0[i]=='Bed' :
			s=a0[i-4];
			if s=='NA' :
				s='0';
			address.append(s);
		elif nAddresses > 0 and a0[i]=='Bath' :
			s=a0[i-4];
			if s=='NA' :
				s='0';
			address.append(s);
		elif nAddresses > 0 and a0[i]=='Sq/Ft' :
			s=a0[i-4].replace(',','');
			if s=='NA' :
				s='0';
			address.append(s);
			# Lot Size, 1 acre = 43560 sq/ft
			s=a0[i+17].replace(',','');
			if " SQ/FT LOT" in s:
				s=s.replace(" SQ/FT LOT",".0");
				lotSize=float(s)/43560.0; # in acres
				s="%.3f"%(lotSize);
			elif "NA LOT" in s :
				s="0.0";
			elif " ACRE LOT" in s :
				s=s.replace(" ACRE LOT","");
			address.append(s);
			i=i+17;
		elif nAddresses > 0 and a0[i]=='class="spanPrice"' :
			i=i+2;
			s=a0[i].replace(',','');
			s=s.replace('$','');
			if s=='NA' or s=='N/A' :
				s='0';
			address.append(s);
			if len(addresses)==0 :
				addresses=[address];
			else:
				addresses.append(address);
		elif 'maxPage="' in a0[i] :
			s=a0[i].replace('maxPage="','');
			s=s.replace('"','');
			#print "s="+s;
			if s.isdigit():
				maxPage=int(s);
			else:
				maxPage=0;
		i=i+1;
	return([maxPage, addresses]);
# End of get_addresses

# Sample of Listing Addresses
#http://www.realtytrac.com/mapsearch/real-estate/al/calhoun-county/p-1?sortbyfield=featured,desc&itemsper=50
# Sample of Sold Addresses
#http://www.realtytrac.com/mapsearch/sold/al/calhoun-county/p-1?sortbyfield=featured,desc&itemsper=50

def get_county(county_name,state_code,saleStatus):
	iPage=1; maxPage=6;
	while iPage <= maxPage :
		if saleStatus==1 : # Listing
			urlAddress='http://www.realtytrac.com/mapsearch/real-estate/'+state_code+'/'+county_name+'/p-'+str(iPage)+'?sortbyfield=featured,desc&itemsper=50'
		else: # Sold
			urlAddress='http://www.realtytrac.com/mapsearch/sold/'+state_code+'/'+county_name+'/p-'+str(iPage)+'?sortbyfield=featured,desc&itemsper=50'
#		urlAddress='http://www.realtytrac.com/mapsearch/real-estate/il/cook-county/glenview/p-'+str(iPage)+'?sortbyfield=featured,desc';
		p=get_addresses(urlAddress, saleStatus, county_name);
		if iPage==1 :
			adrs=p[1];
			maxPage=p[0];
		else:
			for x in p[1]:
				adrs.append(x);
		print county_name+','+state_code+': '+str(iPage)+'/'+str(maxPage)
		iPage=iPage+1;
#		sleep(0.5); # Sleep sometime before hitting the web site again.
		sleep(random.uniform(0.5,1.5)); # Sleep sometime before hitting the web site again.
	return(adrs);
# End of get_county

# Get the command-line arguments.
def getArgv():
	tmp=str(sys.argv);
	tmp=tmp.replace('[','');
	tmp=tmp.replace(']','');
	tmp=tmp.replace("'",'');
	return(tmp.split(","));


# ================= Main Procedure ================= 
argv=getArgv();
saleStatus=int(argv[1]); 
iStart=int(argv[2]); 
iEnd=int(argv[3]);  
# zero based
#
# Read the counties and states.
county_state=[];
with open('county_code.csv', 'r') as csvfile:
	csvFile = csv.reader(csvfile, delimiter=',');
	for row in csvFile:
		if len(county_state)==0:
			county_state=[row];
		else:
			county_state.append(row);
#
tStart=str(datetime.datetime.now());
print 'Query started at '+tStart
#			
# Query addresses.
#saleStatus=2; iStart=51; iEnd=52;  # zero based
outputFile='adrs'+str(saleStatus)+'.csv'
iCounty=iStart; nCounty=0;
while iCounty < iEnd :
	county_name=county_state[iCounty][0];
	state_code=county_state[iCounty][1];
	adrs=get_county(county_name,state_code,saleStatus);
	if len(adrs) > 0 :
		# Print the results.
		if iCounty==0 :
			f=open(outputFile, 'w')
		else:
			f=open(outputFile, 'a')
		for x in adrs :
			i=0;
			for y in x :
				if i==0 :
					f.write(y);
				else:
					f.write(','+y);
				i=i+1;
			f.write('\n');
		f.close();
	nCounty=nCounty+1;
	iCounty=iCounty+1;
	print 'Completion: '+str(iCounty)+'/'+str(iEnd)
#
tEnd=str(datetime.datetime.now());
print 'Query started at '+tStart
print 'Query ended   at '+tEnd

#
#http://www.realtytrac.com/mapsearch/real-estate/mi/charlevoix-county/p-1?sortbyfield=featured,desc&itemsper=50
#resp, html = h.request('http://www.realtytrac.com/mapsearch/real-estate/mi/charlevoix-county/p-1?sortbyfield=featured,desc&itemsper=50', "GET")

