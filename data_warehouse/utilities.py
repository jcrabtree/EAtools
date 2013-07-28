from pandas import *
from datetime import date, datetime, time, timedelta

month1 = {0:'Jan', 1:'Feb', 2:'Mar', 3:'Apr', 4:'May', 5:'Jun', 6:'Jul', 7:'Aug', 8:'Sep', 9:'Oct', 10:'Nov', 11:'Dec'}
month2 = {0:'January',1:'February',2:'March',3:'April',4:'May',5:'June',6:'July',7:'August',8:'September',9:'October',10:'November',11:'December'}

def set_options():
    set_option('display.expand_frame_repr', True)
    set_option('display.line_width',10000)
    set_option('display.max_columns',10000)
    set_option('display.max_rows',10000)
    set_option('display.max_colwidth',10000)
    set_option('display.max_info_columns',10000)
    set_option('display.height',1000)
    set_option('display.max_seq_items',100)

def daily_count(df): #count trading periods in each day
    c = df.fillna(0).groupby(df.fillna(0).index.map(lambda x: x[0])).count()
    return c[c.columns[0]]
    
def time_converter(x):  #Work out the time from the HH number
    return (datetime.combine(date.today(),time(int(np.floor(((int(x)-1)/2.0))),int(((x-1)/2.0 % 1)*60+14.999),59)) + timedelta(seconds=1)).time()
    
def combine_date_time(df): #combine date and time columns, used with .apply
    return datetime.combine(df['date'],df['time'])

def timeseries_convert(df,keep_tp_index=True):
    ''' 
    Convert a MultiIndexed timeseries dataframe from the Data Warehouse 
    into a single datetime indexed timeseries.

    i.e., from Date,Trading_Period index to datetime index (better for plotting in matplotlib)

    Daylight savings is a nuisance, used the CDS Gnash method for this...
    This seems a bit bastardized, surely there is a better way!
    '''
    dc = daily_count(df,)
    tp46 = dc[dc==46].index #short days
    tp50 = dc[dc==50].index #long days
    tp48 = dc[dc==48].index #normal days
    ds = DataFrame(columns=['dls'],index=df.index) #Create temp dataframe for mapping
    ds.ix[ds.index.map(lambda x: x[0].date() in tp46),'dls'] = 46 #set value to 46 on short days
    ds.ix[ds.index.map(lambda x: x[0].date() in tp48),'dls'] = 48 #to 48 on normal days, and,
    ds.ix[ds.index.map(lambda x: x[0].date() in tp50),'dls'] = 50 #to 50 on long days
    ds['date']=ds.index.map(lambda x: x[0]) #create date and trading period columns
    ds['tp']=ds.index.map(lambda x: x[1])
    tp46map=dict(zip(range(1,47),range(1,5)+range(7,49))) #short day mapping
    tp48map=dict(zip(range(1,49),range(1,49))) #normal day mapping
    tp50map=dict(zip(range(1,51),range(1,4)+[4,4.5,5,5.5]+range(6,49))) #long day mapping
    ds['tp1'] = ds[ds['dls']==48].tp.map(lambda x: tp48map[x]) #create new trading period mapping
    ds['tp2'] = ds[ds['dls']==46].tp.map(lambda x: tp46map[x])
    ds['tp3'] = ds[ds['dls']==50].tp.map(lambda x: tp50map[x])
    ds['tp4'] = ds['tp1'].fillna(0) + ds['tp2'].fillna(0)+ds['tp3'].fillna(0)
    ds=ds.drop(['tp1','tp2','tp3'],axis=1)
    ds=ds.rename(columns={'tp4':'tp1'})
    ds['time'] = ds.tp1.map(lambda x: time_converter(x)) #convert from trading period mapping to time
    ds['datetime'] = ds.apply(combine_date_time,axis=1)  #and create datetime
    df['datetime'] = ds['datetime'] #set the df index to the new datetime index
    if keep_tp_index==True:
	    df['tp'] = ds['tp']
    df=df.set_index('datetime')
    return df

  


if __name__ == '__main__':
    pass
