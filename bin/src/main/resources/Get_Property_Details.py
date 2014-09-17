
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

# Get the content of a URL address.
def get_url(urlAddress) :
	tryCount=0;
	while tryCount < 10:
		try:
			h = httplib2.Http();
			resp, html = h.request(urlAddress, "GET")
			break;
		except:
			tryCount=tryCount+1;
			print 'tryCount='+str(tryCount);
			del h;
			sleep(10.0);
	#
	if tryCount > 9:
		return("");
		#print "Error: http request failed.";
		#sys.exit();
	return(html);
	#

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
				s="0";
			elif " ACRE LOT" in s :
				s=s.replace(" ACRE LOT","");
			address.append(s);
			i=i+17;
		elif nAddresses > 0 and a0[i]=='class="spanPrice"' :
			i=i+2;
			s=a0[i].replace(',','');
			s=s.replace('$','');
			if s=='NA' :
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

def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		return False

def month_number(s):
	if s=="Jan" :
		return('1');
	elif s=="Feb" :
		return('2');
	elif s=="Mar" :
		return('3');
	elif s=="Apr" :
		return('4');
	elif s=="May" :
		return('5');
	elif s=="Jun" :
		return('6');
	elif s=="Jul" :
		return('7');
	elif s=="Aug" :
		return('8');
	elif s=="Sep" :
		return('9');
	elif s=="Oct" :
		return('10');
	elif s=="Nov" :
		return('11');
	elif s=="Dec" :
		return('12');
	return('1'); # Default
#

# Get the command-line arguments.
def getArgv():
	tmp=str(sys.argv);
	tmp=tmp.replace('[','');
	tmp=tmp.replace(']','');
	tmp=tmp.replace("'",'');
	return(tmp.split(","));		
		
# ================= Main Procedure ================= 
argv=getArgv();
ZWSID=argv[1].strip();
outputFile=argv[2].strip();
iStart=int(argv[3])-1; # Python is zero-based.
iEnd=int(argv[4])-1;

# Get the property details of an address.
# "http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz1bctzwsojkb_3v2ae&address=608+Revere+Road&citystatezip=Glenview+IL+60025";
#
# Read the state names and abbreviations.
iRow=0;
with open('mover.csv', 'r') as csvfile:
	csvFile = csv.reader(csvfile, delimiter=',');
	for row in csvFile:
		if iRow==0 :
			header=row; # No,lyl_id_no,mover_flag,HSH_ID,address,CTY_NM,STE_CD,ZIP_CD,memberAddress
		elif iRow==1 :
			addresses=[row];
		else:
			addresses.append(row);
		iRow=iRow+1;
# Init
#ZWSID="X1-ZWz1bctzwsojkb_3v2ae";
f=open(outputFile, 'w');
f.write("HSH_ID,Zestimate,Rent Zestimate,#Bedrooms,#Bathrooms,House Size,Lot Size,Year Built,Last Sold Month,Last Sold Year,Last Sold Price,Heating Type,Property tax,URL\n");
#g=open("x.txt", 'w');
nAddressDetail=0;
#
nAddresses=len(addresses);
#for x in addresses :
iAddress=iStart;
while iAddress <= iEnd :
	x=addresses[iAddress];
#	if x[2]=='0' : #mover_flag
#		continue; # Skip non-mover's addresses.
	urlAddress="http://www.zillow.com/webservice/GetSearchResults.htm?zws-id="+ZWSID+"&address="+x[4].strip().replace(' ','+')+"&citystatezip="+x[5].strip().replace(' ','+')+"+"+x[6].strip().replace(' ','+')+"+"+x[7].strip().replace(' ','+');
	html=get_url(urlAddress);
	a0=html_parser(html);
	na0=len(a0);
	i=0;
	while i < na0 :
		if a0[i]=="homedetails" :
			homedetails=a0[i+1];
			break;
		i=i+1;
	#
	if i >= na0 : # Did not receive the link of home details.
		iAddress=iAddress+1;
		continue;
	# 0: HSH_ID
	# 1: Zestimate:
	# 2: Rent Zestimate:
	# 3: Bedrooms:
	# 4: Bathrooms:
	# 5: Single Family:
	# 6: Lot:
	# 7: Year Built:
	# 8: Last Sold month
	# 9: Last Sold Year
	# 10: Last Sold Price
	# 11: Heating Type:
	# 12: Property tax
	# 13: URL
	r=[x[3],'','','','','','','','','','','','',''];
	sleep(0.5);
	html=get_url(homedetails);
	b0=html_parser(html);
	nb0=len(b0);
	i=0;
	while i < nb0 :
#		g.write(str(i)+' : '+b0[i]+'\n');
		if b0[i]=="Zestimate: " in b0[i] : # beginning of new address
			i=i+1;
			while i < nb0 :
				if 'class="value"' in b0[i] :
					break;
				i=i+1;
			i=i+1;
			r[1]=b0[i].replace(',','').replace('$','');
		elif "Rent Zestimate:" in b0[i] :
			i=i+1;
			while i < nb0 :
				if 'class="value"' in b0[i] :
					break;
				i=i+1;
			i=i+1;
			r[2]=b0[i].replace(',','').replace('$','').replace('/mo','');
		elif "Bedrooms:" in b0[i] :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					i=i+1;
					if b0[i]=="span" :
						r[3] = '0';
					else :
						r[3]=b0[i].replace(' beds','').replace(' bed','');
					if not is_number(r[3]) :
						r[3]="0";
					break;
				elif "Contact for details" in b0[i] or 'data-ga-action="Contact' in b0[i] :
					r[3] = '0';
					break;
				i=i+1;
		elif "Bathrooms:" in b0[i] :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					i=i+1;
					if "span" in b0[i] :
						r[4] = "0";
					else :
						r[4]=b0[i].replace(' baths','').replace(' bath','');
					if not is_number(r[4]) :
						r[3]="0";
					break;
				elif "Contact for details" in b0[i] :
					r[4] = '0';
					break;
				i=i+1;
		elif "Single Family:" in b0[i] or "Condo:" in b0[i]:
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					i=i+1;
					r[5]=b0[i].replace(',','').replace(' sq ft','');
					if not is_number(r[5]) :
						r[5]="0";
					break;
				elif "Contact for details" in b0[i] :
					r[5] = '0';
					break;
				i=i+1;
		elif b0[i]=="Lot:" :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					i=i+1;
					if "sqft" in b0[i] :
						x=b0[i].replace(',','').replace(' sqft','');
						lotSize=float(x)/43560.0; # in acres
						r[6]="%.3f"%(lotSize);
					else :
						r[6]=b0[i].replace(',','').replace(' acres','');
					if not is_number(r[6]) :
						r[6]="0";
					break;
				elif "Contact for details" in b0[i] :
					r[6] = "0.0";
					break;
				i=i+1;
		elif b0[i]=="Year Built:" :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					i=i+1;
					r[7]=b0[i];
					if not is_number(r[7]) :
						r[7]="0";
					break;
				elif "Contact for details" in b0[i] :
					r[7] = '0';
					break;
				i=i+1;
		elif b0[i]=="Last Sold:" :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					break;
				i=i+1;
			i=i+1;
			y=b0[i].split(' ');
			r[8]=month_number(y[0]);
			r[9]=y[1];
			if len(y) < 4 :
				r[10]='0';
			else :
				r[10]=y[3].replace(',','').replace('$','');
		elif b0[i]=="Heating Type:" :
			i=i+1;
			while i < nb0 :
				if 'class="prop-facts-value"' in b0[i] :
					break;
				i=i+1;
			i=i+1;
			if "span" in b0[i] :
				r[11] = ''; # Not available
			else :
				r[11]=b0[i].replace(',','-');
		elif b0[i]=="Property tax" :
			i=i+1;
			while i < nb0 :
				if 'class="vendor-cost"' in b0[i] :
					break;
				i=i+1;
			if i < nb0 :
				i=i+2;
				r[12]=b0[i].replace(',','').replace('$','');
			else :
				r[12]="0";
		#
		i=i+1;
	#
	if nAddressDetail==0 :
		addressDetail=[r];
	else :
		addressDetail.append(r);
	nAddressDetail=nAddressDetail+1;
	i=0;
	while i < len(r) :
		if i==0 :
			f.write(r[i]);
		else :
			f.write(","+r[i]);
		i=i+1;
	f.write(homedetails+"\n");
	#
#	if nAddressDetail > 100 :
#		break;
	#
	iAddress=iAddress+1;
	print str(iAddress);
	# Pause before next query.
	sleep(1.0);
#
f.close();
#g.close();
#

