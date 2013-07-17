from pandas import *
from datetime import date, datetime, time, timedelta

def qwop(connection,dateBeg,dateEnd):
    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
    q = """Select
         com.Fp_Offers.DTTM_ID,
         com.Fp_Offers.Trading_DATE,
         com.Fp_Offers.Trading_Period,
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
         com.MAP_Participant_names.Parent_Company_ID = 'MERI' And
         com.MAP_PNode_to_POC_and_Island.Island = 'SI'
      Group By
         com.Fp_Offers.DTTM_ID, com.Fp_Offers.Trading_DATE,
         com.Fp_Offers.Trading_Period, com.MAP_PNode_to_POC_and_Island.Island,
         com.MAP_Participant_names.Parent_Company_ID
      order by
         com.Fp_Offers.DTTM_ID""" % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"))
    t = sql.read_frame(q,connection,coerce_float=True) 
    #t['Date'] = t['Date'].map(lambda x: parsedate(x))
    #t = t.set_index(['Date','TP','node']).price
    #t = t.unstack(level=2)
    return t


def get_prices(connection,dateBeg,dateEnd,tpBeg,tpEnd):
    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
    
    def parsedate(x):
        return dt.datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2]))
    q=r"""Select 
        atomic.DIM_DATE_TIME.DIM_CIVIL_DATE as 'Date',
        atomic.Atm_Spdsolved_Pnodes.period as 'TP',
        atomic.Atm_Spdsolved_Pnodes.pnode as 'node',
        atomic.Atm_Spdsolved_Pnodes.price As 'price'
    From
        atomic.Atm_Spdsolved_Pnodes Inner Join
        atomic.DIM_DATE_TIME On atomic.Atm_Spdsolved_Pnodes.DIM_DTTM_ID =
        atomic.DIM_DATE_TIME.DIM_DATE_TIME_ID
    Where
        atomic.DIM_DATE_TIME.DIM_CIVIL_DATE Between '%s' And '%s' And
        atomic.Atm_Spdsolved_Pnodes.period Between '%s' And '%s' And
        LEN(atomic.Atm_Spdsolved_Pnodes.pnode) > 3 And
        LEN(atomic.Atm_Spdsolved_Pnodes.pnode) < 8
     """ % (dateBeg.strftime("%Y-%m-%d"),dateEnd.strftime("%Y-%m-%d"),tpBeg,tpEnd)
    t = sql.read_frame(q,connection,coerce_float=True) 
    t['Date'] = t['Date'].map(lambda x: parsedate(x))
    t = t.set_index(['Date','TP','node']).price
    t = t.unstack(level=2)
    
    return t

def get_load(connection,dateBeg,dateEnd,tpBeg,tpEnd):
    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
 
    def parsedate(x):
        return dt.datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2]))
    q=r"""Select 
        atomic.DIM_DATE_TIME.DIM_CIVIL_DATE as 'Date',
        atomic.Atm_Spdsolved_Pnodes.period as 'TP',
        atomic.Atm_Spdsolved_Pnodes.pnode as 'node',
        atomic.Atm_Spdsolved_Pnodes.load As 'demand'
    From
        atomic.Atm_Spdsolved_Pnodes Inner Join
        atomic.DIM_DATE_TIME On atomic.Atm_Spdsolved_Pnodes.DIM_DTTM_ID =
        atomic.DIM_DATE_TIME.DIM_DATE_TIME_ID
    Where
        atomic.DIM_DATE_TIME.DIM_CIVIL_DATE Between '%s' And '%s' And
        atomic.Atm_Spdsolved_Pnodes.period Between '%s' And '%s' And
        LEN(atomic.Atm_Spdsolved_Pnodes.pnode) > 3 And
        LEN(atomic.Atm_Spdsolved_Pnodes.pnode) < 8   
     """ % (dateBeg,dateEnd,tpBeg,tpEnd)
    
    t = sql.read_frame(q,connection,coerce_float=True) 
    t['Date'] = t['Date'].map(lambda x: parsedate(x))
    t = t.set_index(['Date','TP','node']).demand
    t = t.unstack(level=2)
    
    return t
    


if __name__ == '__main__':
    pass
