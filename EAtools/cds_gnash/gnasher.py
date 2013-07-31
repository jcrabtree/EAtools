#This file contains functions that interact with Gnash.exe and also additional functions I find myself using repetitively
#D Hume, 3/10/2012
#
#Dependencies, pandas,pbs,datetime.date,datetime.datetime,datetime.time,datetime.timedelta

import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
import pyodbc
import os,sys

if sys.platform.startswith("linux"):
   from pbs import Command

def Gnasher(input_string,output_file): 
    '''Run Gnash from within python session.
	   Note: use tail -f on the Gnash.Trail file to keep tabs on Gnash operation'''
    py_gnash = Command("./Gnash.exe")  #Ok, py_gnash is a callable object in python that will run Gnash.exe
    py_gnash(_in=input_string,_out=output_file)  #_in is the STDIN and _out is the STDOUT! Cool eh?

def GnashChew(datafile): 

   Gin=pd.read_csv(datafile,header = 1,skiprows = [2],na_values = ['?','       ? ','       ?','          ? ','          ?','        ?']) #First read to obtain dump file
   names = Gin.columns #get names used by Gnash
   newnames=[]
   for name in names: 
      name = name.replace('.','_') #replace . in name with _ (not required but makes working with the DataFrame easier, i.e., Gin.Aux_Date can be used instead of Gin['Aux.Date'] to get the data column.
      newnames.append(name)        #create a new names list
   Gin=pd.read_csv(datafile,header = 1,skiprows = [2],na_values = ['?','       ? ','       ?','          ? ','          ?','        ?'],names=newnames, converters={'Aux_DayClock':ordinal_converter},parse_dates=True) #reread in datafile with new names (this is a silly way to do this should use rename with a dictionary object instead!)
   Gin = Gin.set_index('Aux_DayClock')
   return Gin

def ordinal_converter(x):
	x=float(x) + datetime.toordinal(datetime(1899,12,31))
	return datetime(date.fromordinal(int(np.floor(x))).year,date.fromordinal(int(np.floor(x))).month,date.fromordinal(int(np.floor(x))).day,int(np.floor(float("{0:.2f}".format((x%1.0)*24)))),int((float("{0:.2f}".format((x%1.0)*24))%1.0)*60.0))



if __name__ == '__main__':
    pass
