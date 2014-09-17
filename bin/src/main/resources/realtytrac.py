import storm
import sys
import httplib2 # https://code.google.com/p/httplib2/wiki/Examples
#import urllib2 # Fail frequently
import csv
from time import sleep
import datetime
import logging
import time
import random

log = logging.getLogger('RealtyTracSpout')

log.debug('RealtyTracSpout loading')


class RealtyTracSpoutBase(storm.Spout):

    county_state=[];
    index = 0;

    def initialize(self, conf, context):

        with open('county_code.csv', 'r') as csvfile:
            csvFile = csv.reader(csvfile, delimiter=',');
            for row in csvFile:
                if len(self.county_state)==0:
                    self.county_state=[row];
                else:
                    self.county_state.append(row);
        #
        tStart=str(datetime.datetime.now());
        log.debug('Query started at '+tStart);

    def ack(self, id):
        pass

    def fail(self, id):
        pass

    def get_saleStatus(self):
        pass

    def get_sleepTime(self):
        pass


    def nextTuple(self):
        #cs = self.county_state[self.index];
        county_name=self.county_state[self.index][0];
        state_code=self.county_state[self.index][1];
        #storm.emit([county_name]);
        self.index=self.index+1;
        if self.index==len(self.county_state):
            self.index=0;

        self.get_county(county_name, state_code, self.get_saleStatus());
        # for adr in adrs:
        #storm.emit([county_name]);

    def Flip_TF(self,s):
        if s:
            return(False);
        else:
            return(True);
    # End of Flip_TF

    def html_parser(self,a):
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
                sq=self.Flip_TF(sq);
            elif a[iEnd] == '"' :
                dq=self.Flip_TF(dq);
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
    def get_addresses(self,urlAddress, saleStatus, county_name):
        tryCount=0;
        while tryCount < 10:
            try:
                #h = httplib2.Http(".cache");
                h = httplib2.Http();
                resp, html = h.request(urlAddress, "GET")
                break;
            except:
                tryCount=tryCount+1;
                #print 'tryCount='+str(tryCount);
                del h;
                sleep(10.0);
        #response = urllib2.urlopen(urlAddress)
        #html = response.read()
        a0=self.html_parser(html);
        # Init
        nAddresses=0; i=0; j=0; maxPage=0; addresses=[]; address=[''];
        na0=len(a0);

        # Process the data.
        while i < na0 :
            if a0[i]=='class="propertyLink"' : # beginning of new address
                if nAddresses>=1 :
                    storm.emit([address[0],address[1],address[2],address[3],address[4],address[5],address[6],address[7],address[8],address[9], address[10]]);
                nAddresses=nAddresses+1;
                address=["NA","NA","NA","NA","NA",str(saleStatus),"NA","0","0","0","0"];
            elif nAddresses > 0 and a0[i]=="itemprop='streetAddress'" :
                i=i+1;
                s=str(a0[i]);
                s=s.replace(',','');
                address[0]=s;
            elif nAddresses > 0 and a0[i]=="itemprop='addressLocality'" : # City
                i=i+1;
                address[1]=str(a0[i]);
                address[2]=county_name; # Append the county name after the city name.
            #	address.append(str(a0[i]));
            #	address.append(county_name); # Append the county name after the city name.
            elif nAddresses > 0 and a0[i]=="itemprop='addressRegion'" : # State
                i=i+1;
                address[3]=str(a0[i]);
            #	address.append(str(a0[i]));
            elif nAddresses > 0 and a0[i]=="itemprop='postalCode'" : # Zip Code
                i=i+1;
                address[4]=str(a0[i]);
                address[5]=str(saleStatus); # Append the sales status after the ZIP code.
            #	address.append(str(a0[i]));
            #	address.append(str(saleStatus)); # Append the sales status after the ZIP code.
            elif nAddresses > 0 and ((saleStatus==1 and a0[i].strip()=='Listed:') or (saleStatus==2 and a0[i].strip()=='Sold:')) :
                i=i+5;
                address[6]=str(a0[i]);
            #	address.append(str(a0[i]));
            elif nAddresses > 0 and (a0[i].strip()=='Beds' or a0[i].strip()=='Bed') :
                #	print "Beds"
                s=a0[i-4];
                if s=='NA' :
                    s='0';
                address[7]=s;
            #	address.append("Beds="+s);
            elif nAddresses > 0 and (a0[i].strip()=='Baths' or a0[i].strip()=='Bath') :
                #	print "Bath"
                s=a0[i-4];
                if s=='NA' :
                    s='0';
                address[8]=s;
            #	address.append("Bath="+s);
            elif nAddresses > 0 and a0[i].strip()=='sqft' :
                #	print "Sq/Ft"
                s=a0[i-4].replace(',','');
                if s=='NA' or s=='N/A' :
                    s='0';
                address[9]=s;
                #	address.append("Sq/Ft="+s);
                # No more lot size in the page of listing/sold pages.
                # Lot Size, 1 acre = 43560 sq/ft
                #	s=a0[i+17].replace(',','');
                #	if " SQ/FT LOT" in s:
                #		s=s.replace(" SQ/FT LOT",".0");
                #		lotSize=float(s)/43560.0; # in acres
                #		s="%.3f"%(lotSize);
                #	elif "NA LOT" in s :
                #		s="0.0";
                #	elif " ACRE LOT" in s :
                #		s=s.replace(" ACRE LOT","");
                #	address.append("Lot="+s);
                i=i+17;
            #	elif nAddresses > 0 and a0[i]=='class="spanPrice"' :
            elif nAddresses > 0 and a0[i]=='itemprop="price"' :
                i=i+2;
                s=a0[i].replace(',','');
                s=s.replace('$','');
                if s=='NA' or s=='N/A' :
                    s='0';
                address[10]=s;
            #	address.append(s);
            #	if len(addresses)==0 :
            #		addresses=[address];
            #	else:
            #		addresses.append(address);
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

    def get_county(self,county_name,state_code,saleStatus):

        iPage=1; maxPage=6;
        while iPage <= maxPage :
            if saleStatus==1 : # Listing
                urlAddress='http://www.realtytrac.com/mapsearch/real-estate/'+state_code+'/'+county_name+'/p-'+str(iPage)+'?sortbyfield=featured,desc&itemsper=50';
            else: # Sold
                urlAddress='http://www.realtytrac.com/mapsearch/sold/'+state_code+'/'+county_name+'/p-'+str(iPage)+'?sortbyfield=featured,desc&itemsper=50'
            #		urlAddress='http://www.realtytrac.com/mapsearch/real-estate/il/cook-county/glenview/p-'+str(iPage)+'?sortbyfield=featured,desc';
            p=self.get_addresses(urlAddress, saleStatus, county_name);
            if iPage==1 :
                adrs=p[1];
                maxPage=p[0];
            else:
                for x in p[1]:
                    adrs.append(x);
            #print county_name+','+state_code+': '+str(iPage)+'/'+str(maxPage)
            iPage=iPage+1;
            sleep(self.get_sleepTime()); # Sleep sometime before hitting the web site again.
        return(adrs);
    # End of get_county



class RealtyTracSpoutForSold(RealtyTracSpoutBase):
    def get_saleStatus(self):
        return 2;

    def get_sleepTime(self):
        return random.uniform(1,3);


class RealtyTracSpoutForListed(RealtyTracSpoutBase):
    def get_saleStatus(self):
        return 1;

    def get_sleepTime(self):
        return random.uniform(2,6);