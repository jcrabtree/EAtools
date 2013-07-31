import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
import pyodbc
import os,sys
from EAtools.EAstyles.ea_styles import ea_p,ea_s

month1 = {0:'Jan', 1:'Feb', 2:'Mar', 3:'Apr', 4:'May', 5:'Jun', 6:'Jul', 7:'Aug', 8:'Sep', 9:'Oct', 10:'Nov', 11:'Dec'}
month2 = {0:'January',1:'February',2:'March',3:'April',4:'May',5:'June',6:'July',7:'August',8:'September',9:'October',10:'November',11:'December'}

def set_options():
    pd.set_option('display.expand_frame_repr', True)
    pd.set_option('display.line_width',10000)
    pd.set_option('display.max_columns',10000)
    pd.set_option('display.max_rows',10000)
    pd.set_option('display.max_colwidth',10000)
    pd.set_option('display.max_info_columns',10000)
    pd.set_option('display.height',1000)
    pd.set_option('display.max_seq_items',100)

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
    ds = pd.DataFrame(columns=['dls'],index=df.index) #Create temp dataframe for mapping
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

#Functions for munging up Comit Hydro data

def calc_mean_percentile(data,percentile_width,future=False):
    '''This function, given a historic timeseries, returns a time series dataframe that includes the historic 
       daily mean and percentiles of the given data.
       The function returns a repeating annual dataframe indexed from; the start date of the data supplied to either:
           (a) the end date of the given data (if future==False), or;
           (b) out to the end of next year, (if future==True), where next year is the year after the function is called.
       
       The columns returned are; mean, and the upper and lower percentiles (based on the given percentile width).'''

    
    def time_serializer(annual_stats,index):
        '''Generate a dummy dataframe, indexed with given index''' 
        df_index = pd.DataFrame(index=index)
        for col in annual_stats.columns:
            df_index[col] = df_index.index.map(lambda x: annual_stats.ix[(x.month,x.day),col])
        return df_index
    
    data_grouped = data.groupby([lambda x: x.month,lambda y: y.day]) #grouped my month and day
    data_mean = data_grouped.mean()
    lower_percentile = data_grouped.describe(percentile_width=percentile_width)
    lpn = 50 - percentile_width/2.0
    upn = 50 + percentile_width/2.0
    lpn=np.unique(list(lower_percentile.index.get_level_values(2)))[0]       
    upn=np.unique(list(lower_percentile.index.get_level_values(2)))[2]  
    lower_percentile = lower_percentile.ix[lower_percentile.index.get_level_values(2)==lpn].reset_index().rename(columns={'level_0':'month','level_1':'day'}).set_index(['month','day'])
    del lower_percentile['level_2']
    upper_percentile = data_grouped.describe(percentile_width=percentile_width)
    upper_percentile = upper_percentile.ix[upper_percentile.index.get_level_values(2)==upn].reset_index().rename(columns={'level_0':'month','level_1':'day'}).set_index(['month','day'])
    del upper_percentile['level_2']
    data_cat = pd.DataFrame({'mean':data_mean,\
                            (("%sth percentile"% lpn.replace('.0','')).replace('%','')):lower_percentile[0],\
                            (("%sth percentile"% upn.replace('.0','')).replace('%','')):upper_percentile[0]}) #historic data
    
    if future == False:
        #Create full indexed dummy series all the way back to 1932...to today
        data_cat = time_serializer(data_cat,data.index)
        data_cat['Actual'] = data

    else: #create a dummy index out till the end of the next year
        daily_index = pd.date_range(data.index[0],datetime(datetime.now().year+1,12,31), freq='D')
        data_cat = time_serializer(data_cat,daily_index)
        data_cat['Actual'] = data

    return data_cat

def panel_beater(storage,inflow,days,percentile_width=80):
    '''Given Storage and Inflow time series data, this function returns a panel object for each catchment containing:
           Items: Storage and Inflows 
           Major_axis axis: dates for the last x days of the data
           Minor_axis axis: 10th percentile,90th percentile, mean and Actual for each item.'''
    storage = calc_mean_percentile(storage,percentile_width)
    inflows = calc_mean_percentile(inflow,percentile_width)
    return pd.Panel({'Storage':storage.tail(days),'Inflows':inflows.tail(days)})

#System Operator HRC downloader

def get_SO_HRC(link):
    '''This function downloads the SO hydro rick curves, returning pandas dataframe objects for the SI and NZ.
    Note: This worked on 01/08/2013, no guarantees it will work into the future as dependend on xlsx file format'''
    HRC_SO = pd.ExcelFile(urllib2.urlopen(link))
    NZ = HRC_SO.parse(HRC_SO.sheet_names[0], header=3).T.ix[:,:6].T
    NZ = NZ.rename(index = dict(zip(NZ.index,['1%','2%','4%','6%','8%','10%']))).T.applymap(lambda x: float(x))
    SI = HRC_SO.parse(HRC_SO.sheet_names[0], header=3).T.ix[:,9:15].T
    SI = SI.rename(index = dict(zip(SI.index,['1%','2%','4%','6%','8%','10%']))).T.applymap(lambda x: float(x))
    return NZ,SI

#Hydrology plot functions

def hydrology_plot(panel,title=None):
    import matplotlib.pyplot as plt
    fig = plt.figure(1,figsize=[25,20])
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212, sharex=ax1)

    def hydro_plotter(df,ax,ylabel,title,colour1,colour2,colour3):
        x = df.index
        up = df.columns[0]
        lp = df.columns[1]
        act = df.columns[3]
        ax.fill_between(x,df[lp],df[up],color=colour2)
        df['mean'].plot(ax=ax,linewidth = 3,color=colour1)
        df[act].plot(ax=ax,linewidth = 3,color=colour3)
        xlabels = ax.get_xticklabels() 
        for label in xlabels: 
            label.set_rotation(0) 
        ax.set_xlabel('')
        ax.set_ylabel(ylabel)
        ax.legend()
        a = plt.Line2D((0,1),(0,0), color=colour3)
        m = plt.Line2D((0,1),(0,0), color=colour1)
        p = plt.Rectangle((0, 0), 1, 1, color=colour2)
        ax.legend([a,m,p], ["Actual","Mean since 1932","10th-90th percentile"])
    ota_colour1 = ea_s['or1']
    ota_colour2 = ea_s['or2']
    ota_colour3 = ea_s['rd1']
    ben_colour1 = ea_s['bl1']
    ben_colour2 = ea_s['be1']
    ben_colour3 = ea_s['pp1']

    hydro_plotter(panel['Storage'],ax1,'Storage (GWh)',title,ben_colour1,ben_colour2,ben_colour3)
    hydro_plotter(panel['Inflows'],ax2,'Inflows (GWh/day)',title,ota_colour1,ota_colour2,ota_colour3)



if __name__ == '__main__':
    pass
