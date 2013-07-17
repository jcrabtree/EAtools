from pandas import *
from datetime import date, datetime, time, timedelta

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
