from pandas import *

def time_converter(x):
    return time(int(np.floor(((int(x)-1)/2.0))),int(((int(x)-1)/2.0 % 1)*60+15)) #Work out the time from the HH number

def daily_count(df):
    c = df.fillna(0).groupby(df.fillna(0).index.map(lambda x: x[0])).count()
    return c[c.columns[0]]
def time_converter(x):
    return (datetime.combine(date.today(),time(int(np.floor(((int(x)-1)/2.0))),int(((x-1)/2.0 % 1)*60+14.999),59)) + timedelta(seconds=1)).time()#Work out the time from the HH number
def combine_date_time(df):
    return datetime.combine(df['date'],df['time'])

def timeseries_convert(df):
    ''' 
    Convert a MultiIndexed timeseries dataframe from the Data Warehouse 
    into a single datetime indexed timeseries.

    i.e., from Date,Trading_Period index to datetime index

    Daylight savings is a nuisance, we used the CDS Gnash method for this...
    This seems a bit bastardized, surely there is a better way!
    '''

    dc = daily_count(df)
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
    df=df.set_index('datetime')
    return df

if __name__ == '__main__':
    pass
