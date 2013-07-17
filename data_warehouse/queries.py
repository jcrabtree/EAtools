from pandas import *
from datetime import date, datetime, time, timedelta

def get_ramu_summary(connection,dateBeg,dateEnd):
    '''island energy, reserve and hvdc summary'''

    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
    def parsedate(x):
        return dt.datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2]))

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
    t['Date'] = t['Date'].map(lambda x: parsedate(x))
    t = t.set_index(['Date','TP','island'])
    del t['DIM_DTTM_ID']
    return t




def get_rm_generation(connection,dateBeg,dateEnd,company):
    '''rm generation by parent company, from Ramu'''
    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
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
    #t['Date'] = t['Date'].map(lambda x: parsedate(x))
    #t = t.set_index(['Date','TP','node']).price
    #t = t.unstack(level=2)
    return t

def get_rm_demand(connection,dateBeg,dateEnd,company):
    '''rm demand by parent company, from Ramu'''
    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
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
    #t['Date'] = t['Date'].map(lambda x: parsedate(x))
    #t = t.set_index(['Date','TP','node']).price
    #t = t.unstack(level=2)
    return t



def get_qwop(connection,dateBeg,dateEnd,company):
    '''Quantity weighted offer price query, from Ramu'''
    def parsedate(x):
        return dt.datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2]))

    import pandas.io.sql as sql
    import pyodbc
    import datetime as dt
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
    t['Date'] = t['Date'].map(lambda x: parsedate(x))
    t = t.set_index(['Date','TP']).QWOP
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
