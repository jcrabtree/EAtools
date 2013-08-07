"""
The is cloned from NigelClelands version
From https://github.com/NigelCleland/nzem.git

Interface for interacting with the Gnash DB as set out by the Authority.

Based upon the work begun by David Hume in the EATools repository:
https://github.com/ElectricityAuthority/EAtools

Assumes you have the CDS extracted onto your computer


DAVES NOTES: (07/08/2013)

Currently have not been able to get the output of Gnash passed successfully to a StingIO object.
I think there may be an encoding issue as Gnash is not UTF-8 compliant and uses a funny decimal point

For now, we are dumping output to a temp.txt file, then reading this and doing .replace('\xfa','.')

Other modifications require that the Gnash.Awake.txt file looks like this:

set DUMP.AUX.PROVENANCE      = F
set DUMP.AUX.NAMES   = T
set DUMP.AUX.TITLES  = F
set DUMP.AUX.DATE    = T
set DUMP.AUX.TIME    =    0
set DUMP.AUX.DTUNITED        = F
set DUMP.AUX.HHN     = T
set DUMP.AUX.DAYCLOCK        = T
set DUMP.AUX.DATE = F
set DUMP.AUX.DAYCLASSCODE    = F
set DUMP.AUX.DAYLIGHTSAVING  = F
set DUMP.AUX.SOLAR.AZIMUTH = F
set DUMP.AUX.SOLAR.ALTITUDE        = F
set DUMP.WITH.GANGRENE        = T
set DUMP.WITH.ZOMBIES = T
set DUMP.WITH.GHOSTS  = F
set DUMP.WITH.HHJIG   = T
set BLABBERMOUTH    = T


"""

import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
#import pyodbc
import os
import sys
import subprocess
from cStringIO import StringIO
import time

if sys.platform.startswith("linux"):
   from pbs import Command
   #from sh import Command

# Change to the gnash directory, assumes it is extracted in the user home path.

cwd = os.getcwd()

#gnash_path = os.path.join(os.path.expanduser('~'+cds_path), 'CDS', 'CentralisedDataset', 'HalfHourly')
gnash_path = '/home/humed/python/gnash/cds/'
os.chdir(gnash_path)

class Gnasher(object):
    """Gnasher is a class designed to make interfacing with the Gnash.exe as 
    painless as possible. It should eventually handle constructing queries,
    returning data as pandas DataFrames and generally taking care of
    the BS which makes dealing with such systems "fun"
    """
    def __init__(self):
        super(Gnasher, self).__init__() #inherit __init__ from object 

    def query_gnash(self, input_string):
        
        self._run_query(input_string) 
        self._scrub_output()
        self.query = self._convertgnashdump()

    def _run_query(self, input_string):
        # Trying to make the buffering process working.
        try:
            self.output = StringIO()
            #def grab_output(line): #hopefully we can get this working in the future...
            #    print line
            #    self.output.write(line.replace('\xfa','.'))
            #self.gnash(_in=input_string, _out=grab_output).wait()
            self.gnash = Command(gnash_path +  "Gnash.exe")
            self.gnash(_in=input_string, _out="temp.txt").wait()

        except:
            print "Error, cannot run the query on Gnash"

    
    def _convertgnashdump(self):

        na_conv = lambda x: np.nan if "?" in str(x) else x

        # First read to obtain dump file
        Gin=pd.read_csv(self.output, header=0)
        # Dictionary Comprehension to rename columns
        new_names = {x: x.replace('.', '_') for x in Gin.columns}
        Gin.rename(columns=new_names, inplace=True)
        Gin = Gin.applymap(na_conv)
        # Drop na values
        Gin = Gin.dropna()
        # Set a new index
        Gin["Aux_DayClock"] = Gin["Aux_DayClock"].apply(self._ordinal_converter)
        Gin = Gin.set_index(['Aux_DayClock','Aux_HHn'])
        return Gin

    def _ordinal_converter(self, x):
        if np.isnan(x):
            return np.nan
    
        x =float(x) + datetime.toordinal(datetime(1899,12,31))
        ord_date = date.fromordinal(int(np.floor(x)))
         
        return datetime(ord_date.year,
                        ord_date.month,
                        ord_date.day,
                        int(np.floor((x % 1.0) * 24)),
                        int(np.round((x % 1.0 * 24 % 1.0 * 60), decimals=0)))

    def _scrub_output(self):
        # Go line by line through the output until you find the header, hopefully we can get this working in future...
        #string = self.output.getvalue()
        #self.output.close()
        #begin = string.find("Aux.Date") - 1
        #end = string.find("Gnash:Bye") - 2
        #string = string[begin:end]
        #self.output = StringIO(string)
        
        output = StringIO()
        f = open('temp.txt','r')
        for l in f:
            output.write(l)
        string = output.getvalue()
        output.close()

        begin = string.find("Aux.HHn") - 1
        end = string.find("Gnash:Bye") - 2
        string = string[begin:end].replace('\xfa','.')
        self.output = StringIO(string)


