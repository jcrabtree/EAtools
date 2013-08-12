import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
import pyodbc
import os,sys
import urllib2
import xlrd
from EAtools.data_warehouse.utilities import date_converter,timeseries_convert

########################################################################
#Data Warehouse connection setup
########################################################################

def DW_connect(linux=False,DSN='DWMarketData'):
    '''Connect to the EA Datawarehouse from window and linux boxes:
       Note: DSN can also be: NZxDaily_LIVE, or HH'''
    if linux == True:
        con = pyodbc.connect('DSN=' + DSN + ';UID=linux_user;PWD=linux')
    else:
        con = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=eadwprod\live;DATABASE=' + DSN + ';UID=linux_user;PWD=linux')
    
    return con

########################################################################
#Final Pricing SQL queries
########################################################################

#First we define some simple deocator functions.  Note this is my first 
#play with decorators (2/8/2013)
def FP_price(fn):
    def replacer(*args, **kwargs):
        return fn(*args, **kwargs) \
                   .replace('column_name','FP_energy_price') \
                   .replace('data_name','price') \
                   .replace('table_name','FP_price')
    return replacer

def FP_demand(fn):
    def replacer(*args, **kwargs):
        return fn(*args, **kwargs) \
                   .replace('column_name','FP_demand') \
                   .replace('data_name','demand') \
                   .replace('table_name','FP_demand')
    return replacer

#This returns a default query string that we will attempt to decorate 
#depending if we want price or demand
def FP_query(dateBeg,dateEnd,tpBeg=1,tpEnd=50,nodelist=None):
    q1 = """SELECT dw.Trading_Date as 'Date',
                     dw.Trading_period as 'TP',
                     dw.PNode,
                     dw.column_name as 'data_name'
          FROM
                 com.table_name dw
          WHERE
                 dw.Trading_Date between '%s' and '%s' 
            and
                 dw.Trading_period between '%s' and '%s' """ 
    if nodelist:
        q2=""" and dw.PNode in %s ORDER BY [Trading_Date],[Trading_period] Asc""" 
        if len(nodelist) > 1:
            node_str = str(tuple(nodelist))
        else:
            node_str = "('" + nodelist[0] + "')"
        q=(q1+q2) % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),tpBeg,tpEnd,node_str)
        q = q.replace('\n\n','\n')
    else:
        q2 = """ and
                     LEN(dw.PNode) > 3 And
                     LEN(dw.PNode) < 8   
              ORDER BY [Date],[TP]  Asc""" 
        q = (q1+q2) % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),tpBeg,tpEnd)
        q = q.replace('\n\n','\n')
    return q

#Hook into the DW with the query string q
def FP_getter(connection,q): 
    from time import clock
    tic = clock()
    s = sql.read_frame(q,connection,coerce_float=True) 
    if type(s.Date[0]) is unicode: #Need to test this as either different versions or operating systems appear to have an effect on the date parsing...
        s['Date'] = s['Date'].map(lambda x: date_converter(x))
    s = s.set_index(['Date','TP','PNode']).unstack(level=2)
    toc = clock()
    print "Took %s seconds to retrieve and convert data from warehouse!" % str(toc-tic)
    return s

#Ok, now we decorate our default FP_query function to create two new functions: 
query_prices = FP_price(FP_query)  #for prices
query_demand = FP_demand(FP_query) #for demand...

########################################################################
#LWAP data used in weekly report
########################################################################

def get_lwaps(connection):

    #From the start of last month, to yesterday
    dBegt0 = datetime(datetime.now().date().year,datetime.now().date().month-1,1).date()
    dEndt0 = datetime.now().date()-timedelta(days=1)
    #From the start of last month, last year, to the end of the next month, last year...
    dBegt1 = datetime(datetime.now().date().year-1,datetime.now().date().month-1,1).date()
    dEndt1 = datetime(datetime.now().date().year-1,datetime.now().date().month+2,1).date()-timedelta(days=1)
    #From the start of last month, 2 years ago, to the end of the next month 2 years ago...
    dBegt2 = datetime(datetime.now().date().year-2,datetime.now().date().month-1,1).date()
    dEndt2 = datetime(datetime.now().date().year-2,datetime.now().date().month+2,1).date()-timedelta(days=1)

    l0 = timeseries_convert(FP_getter(connection,query_demand(dBegt0,dEndt0)))
    p0 = timeseries_convert(FP_getter(connection,query_prices(dBegt0,dEndt0)))
    l1 = timeseries_convert(FP_getter(connection,query_demand(dBegt1,dEndt1)))
    p1 = timeseries_convert(FP_getter(connection,query_prices(dBegt1,dEndt1)))
    l2 = timeseries_convert(FP_getter(connection,query_demand(dBegt2,dEndt2)))
    p2 = timeseries_convert(FP_getter(connection,query_prices(dBegt2,dEndt2)))

    def lwap(p,l):
        return (p*l).sum(axis=1)/l.sum(axis=1)

    lwap0 = lwap(p0.price,l0.demand)
    lwap1 = lwap(p1.price,l1.demand)
    lwap2 = lwap(p2.price,l2.demand)

    def shift_index(df,year_shift):
        return df.index.map(lambda x: datetime(x.year+year_shift,x.month,x.day,x.hour,x.minute))
  
    lwap1.index = shift_index(lwap1,1)
    lwap2.index = shift_index(lwap2,2)
    lwaps = pd.DataFrame({str(dBegt0) + ' to ' + str(dEndt0):lwap0,str(dBegt1) + ' to ' + str(dEndt1):lwap1,str(dBegt2) + ' to ' + str(dEndt2):lwap2})
    return lwaps



########################################################################
#Comit hydro data - currently not in the DW!
########################################################################

def get_comit_data(inflow_pickle,storage_pickle,since=1932,catchments = ['Taupo','Tekapo','Pukaki','Hawea','TeAnau','Manapouri']):
    '''This function reads and processes Comit Hydro data.
      
       Note: it is planned that this data will in the future be included 
       in the Data Warehouse, in which case this function will change to
       an SQL query.'''
    inflows = pd.read_pickle(inflow_pickle)
    storage = pd.read_pickle(storage_pickle)
    inflows=inflows[inflows.index.map(lambda x: x.year)>=since] #take data since 1932
    storage=storage[storage.index.map(lambda x: x.year)>=since]
    inflows=inflows*24.0/1000.0 #convert to GWh
    return inflows.ix[:,catchments],storage.ix[:,catchments]

########################################################################
#System Operator HRC downloader - currently not in the DW!
########################################################################

def get_SO_HRC(link):
    '''This function downloads the SO hydro risk curves, returning 
       pandas dataframe objects for the SI and NZ.
       Note: This worked on 01/08/2013, no guarantees it will work into 
             the future as dependent on xlsx file format'''
    HRC_SO = pd.ExcelFile(urllib2.urlopen(link))
    NZ = HRC_SO.parse(HRC_SO.sheet_names[0], header=3).T.ix[:,:6].T
    NZ = NZ.rename(index = dict(zip(NZ.index,['1%','2%','4%','6%','8%','10%']))).T.applymap(lambda x: float(x))
    SI = HRC_SO.parse(HRC_SO.sheet_names[0], header=3).T.ix[:,9:15].T
    SI = SI.rename(index = dict(zip(SI.index,['1%','2%','4%','6%','8%','10%']))).T.applymap(lambda x: float(x))
    return NZ,SI

########################################################################
#EA HRC getter - currently not in the DW!
########################################################################

def get_EA_HRC(csv_file):
    '''This function downloads the EA hydro risk curves.  Although this 
       data comes from the SO, these differ in that ex-post (historic) 
       changes to the HRC data are, at least, attempted to be preserved.
       This is not the case for the SO data which is very much an ex-
       anti, forward looking curve that over-writes historic changes (as
       observed at the time).  
       
       This returns pandas dataframe objects for the SI and NZ.
    
       Note: This worked on 01/08/2013, no guarantees it will work into 
             the future as dependent on the csv file format'''
    NZ = pd.read_csv(csv_file,index_col=0,parse_dates=True,dayfirst=True,usecols=[0,1,2,3,4,5,6]).drop_duplicates()
    SI = pd.read_csv(csv_file,index_col=0,parse_dates=True,dayfirst=True,usecols=[0,8,9,10,11,12,13])
    SI = SI.reset_index().rename(columns=dict(zip(SI.reset_index().columns,['South Island','1%','2%','4%','6%','8%','10%']))).set_index('South Island').drop_duplicates()
    return NZ,SI

def get_ramu_summary(connection,dateBeg,dateEnd):
    '''island energy, reserve and hvdc summary'''
 
    q="""Select
         atomic.Atm_Spdsolved_Islands.DIM_DTTM_ID,
         atomic.Atm_Spdsolved_Islands.DATA_DATE As 'Date',
         atomic.Atm_Spdsolved_Islands.PERIOD As 'TP',
         atomic.Atm_Spdsolved_Islands.island,
         atomic.Atm_Spdsolved_Islands.reference_price,
         atomic.Atm_Spdsolved_Islands.reserve_price_six_sec,
         atomic.Atm_Spdsolved_Islands.reserve_price_sixty_sec,
         atomic.Atm_Spdsolved_Islands.energy_offered,
         atomic.Atm_Spdsolved_Islands.energy_cleared,
         atomic.Atm_Spdsolved_Islands.[load],
         atomic.Atm_Spdsolved_Islands.net_hvdc_interchange,
         atomic.Atm_Spdsolved_Islands.max_nodal_price,
         atomic.Atm_Spdsolved_Islands.min_nodal_price,
         atomic.Atm_Spdsolved_Islands.max_offer_price,
         atomic.Atm_Spdsolved_Islands.six_sec_risk_node,
         atomic.Atm_Spdsolved_Islands.sixty_sec_risk_node,
         atomic.Atm_Spdsolved_Islands.six_sec_risk,
         atomic.Atm_Spdsolved_Islands.sixty_sec_risk,
         atomic.Atm_Spdsolved_Islands.cleared_reserve_six_sec,
         atomic.Atm_Spdsolved_Islands.cleared_reserve_sixty_sec
    From
         atomic.Atm_Spdsolved_Islands Inner Join
         atomic.DIM_DATE_TIME On atomic.Atm_Spdsolved_Islands.DIM_DTTM_ID =
         atomic.DIM_DATE_TIME.DIM_DATE_TIME_ID
    Where
         atomic.DIM_DATE_TIME.DIM_CIVIL_DATE >= '%s' And
         atomic.DIM_DATE_TIME.DIM_CIVIL_DATE <= '%s' """ % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"))
    t = sql.read_frame(q,connection,coerce_float=True) 
    t['Date'] = t['Date'].map(lambda x: date_converter(x))
    t = t.set_index(['Date','TP','island'])
    del t['DIM_DTTM_ID']
    return t

def get_rm_generation(connection,dateBeg,dateEnd,company):
    '''rm generation by parent company, from Ramu'''
    q = """Select
       com.RM_Generation_by_trader.DTTM_ID,
       com.RM_Generation_by_trader.POC,
       com.MAP_Participant_names.Parent_Company_ID,
       Sum(com.RM_Generation_by_trader.RM_generation) As 'RMGen'
    From
       com.MAP_Participant_names Inner Join
       com.RM_Generation_by_trader On com.RM_Generation_by_trader.Trader_ID =
       com.MAP_Participant_names.Trader_Id
    Where
       com.RM_Generation_by_trader.Trading_Date >= '%s' And
       com.RM_Generation_by_trader.Trading_Date <= '%s' And
       com.MAP_Participant_names.Parent_Company_ID Like '%s'
    Group By
       com.RM_Generation_by_trader.DTTM_ID, com.RM_Generation_by_trader.POC,
       com.MAP_Participant_names.Parent_Company_ID
    Order By
       com.RM_Generation_by_trader.DTTM_ID,
       com.MAP_Participant_names.Parent_Company_ID,
       com.RM_Generation_by_trader.POC""" % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),company)
    t = sql.read_frame(q,connection,coerce_float=True) 
    return t

def get_rm_demand(connection,dateBeg,dateEnd,company):
    '''rm demand by parent company, from Ramu'''
    q = """Select
        com.RM_Demand_by_trader.DTTM_ID,
        com.RM_Demand_by_trader.Trading_Date,
        com.RM_Demand_by_trader.Trading_Period,
        com.MAP_Participant_names.Parent_Company_ID,
        com.MAP_NSP_POC_to_region.ISLAND,
        Sum(com.RM_Demand_by_trader.RM_demand) As 'RMLoad'
   From
        com.RM_Demand_by_trader Inner Join
        com.MAP_Participant_names On com.RM_Demand_by_trader.Trader_ID =
          com.MAP_Participant_names.Trader_Id Inner Join
        com.MAP_NSP_POC_to_region On com.RM_Demand_by_trader.POC =
          com.MAP_NSP_POC_to_region.POC
   Where
        com.RM_Demand_by_trader.Trading_Date >= '%s' And
        com.RM_Demand_by_trader.Trading_Date <= '%s' And
        com.MAP_Participant_names.Parent_Company_ID = '%s'
   Group By
        com.RM_Demand_by_trader.DTTM_ID, com.RM_Demand_by_trader.Trading_Date,
        com.RM_Demand_by_trader.Trading_Period,
        com.MAP_Participant_names.Parent_Company_ID, com.MAP_NSP_POC_to_region.ISLAND
   Order By
        com.RM_Demand_by_trader.DTTM_ID,
        com.MAP_NSP_POC_to_region.ISLAND""" % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),company)
    t = sql.read_frame(q,connection,coerce_float=True) 
    return t



def get_qwop(connection,dateBeg,dateEnd,company):
    '''Quantity weighted offer price query, from Ramu'''
    q = """Select
         com.Fp_Offers.DTTM_ID,
         com.Fp_Offers.Trading_DATE as 'Date',
         com.Fp_Offers.Trading_Period as 'TP',
         com.MAP_PNode_to_POC_and_Island.Island,
         com.MAP_Participant_names.Parent_Company_ID,
         (Sum((com.Fp_Offers.Offer_Price * com.Fp_Offers.Offer_Quantity)) /
          Sum(com.Fp_Offers.Offer_Quantity)) As 'QWOP'
      From
         com.Fp_Offers Inner Join
         com.MAP_Participant_names On com.Fp_Offers.Trader_Id =
         com.MAP_Participant_names.Trader_Id Inner Join
         com.MAP_PNode_to_POC_and_Island On com.Fp_Offers.PNode =
         com.MAP_PNode_to_POC_and_Island.PNode
      Where
         com.Fp_Offers.Trading_DATE >= '%s' And
         com.Fp_Offers.Trading_DATE <= '%s' And
         com.Fp_Offers.trade_type = 'ENOF' And
         com.MAP_Participant_names.Parent_Company_ID = '%s' And
         com.MAP_PNode_to_POC_and_Island.Island = 'SI'
      Group By
         com.Fp_Offers.DTTM_ID, com.Fp_Offers.Trading_DATE,
         com.Fp_Offers.Trading_Period, com.MAP_PNode_to_POC_and_Island.Island,
         com.MAP_Participant_names.Parent_Company_ID
      order by
         com.Fp_Offers.DTTM_ID""" % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),company)
    t = sql.read_frame(q,connection,coerce_float=True) 
    t['Date'] = t['Date'].map(lambda x: date_converter(x))
    t = t.set_index(['Date','TP']).QWOP
    return t




if __name__ == '__main__':
    pass
