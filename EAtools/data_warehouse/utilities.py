import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
import pyodbc
import os,sys
from EAtools.EAstyles.ea_styles import ea_p,ea_s
#from EAtools.data_warehouse.queries import FP_getter,query_demand
from bs4 import BeautifulSoup
import mechanize

import matplotlib.pyplot as plt

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

def date_converter2(x):
    return datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2])) #Work out the time from the HH number

def date_converter(x):
    return datetime(int(x.split('-')[0]),int(x.split('-')[1]),int(x.split('-')[2])).date() #but only the date...

def combine_date_time(df): #combine date and time columns, used with .apply
    return datetime.combine(df['date'],df['time'])

def append_day(df):
    df = df.append(df.tail(1))
    index = [df.index.tolist()[:-1] + [df.index.tolist()[-1]+timedelta(days=1)]]
    df.index =index
    return df 
    
def append_quarter(df):
    df = df.append(df.tail(1))
    index = pd.period_range(df.index[0].start_time, df.index[-1].end_time+timedelta(days=1), freq='Q')
    df.index = index
    return df
    
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
    #need to test that the index is a datetime type
    if str(type(df.index[0][0]))=="<type 'datetime.date'>":  #this appears to have some dependence on the windows/linux systems...
        ds.ix[ds.index.map(lambda x: x[0] in tp46),'dls'] = 46 #set value to 46 on short days
        ds.ix[ds.index.map(lambda x: x[0] in tp48),'dls'] = 48 #to 48 on normal days, and,
        ds.ix[ds.index.map(lambda x: x[0] in tp50),'dls'] = 50 #to 50 on long days
    else:
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


#Hydrology plot functions

def hydrology_plot(figno,panel,fig_file,title=None):
    fig = plt.figure(figno,figsize=[30,17])
    ax1 = plt.subplot2grid((3, 1), (0, 0), rowspan=2)
    ax2 = plt.subplot2grid((3, 1), (2, 0))
    #ax1 = fig.add_subplot(211)
    #ax2 = fig.add_subplot(212, sharex=ax1)

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
    fig.tight_layout()
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)

#HRC plotter
def hrc_plot(figno,hrc,actual,means,fig_file):
           
    fig = plt.figure(figno,figsize=[25,20])
    ax1 = fig.add_subplot(111)
    colours = {'1%':ea_p['yl2'],'2%':ea_p['yl1'],'4%':ea_s['or2'],'6%':ea_s['or1'],'8%':ea_p['rd2'],'10%':ea_p['rd1']}
    
    def hrc_plotter(HRC,actual,means,ax,ylabel,colours):

        x = HRC.index
        for c in HRC.columns:
            HRC[c].plot(color=colours[c])
            #ax.fill_between(x,HRC[c],color=colours[c])
            xlabels = ax.get_xticklabels() 
        HRC['10%'].plot(color=ea_p['rd1'],linewidth=4)
        means.plot(color=ea_s['gy2'],linewidth=6)
        actual.plot(color=ea_p['bl1'],linewidth=4)
        for label in xlabels: 
            label.set_rotation(0) 
        ax.set_xlabel('')
        ax.set_ylabel(ylabel)
        act1 = plt.Line2D((0,1),(0,0), color=ea_p['bl1'],linewidth=4)
        mean1= plt.Line2D((0,1),(0,0), color=ea_p['gy2'],linewidth=6)
        hrc1 = plt.Line2D((0,1),(0,0), color=colours['1%'])
        hrc2 = plt.Line2D((0,1),(0,0), color=colours['2%'])
        hrc4 = plt.Line2D((0,1),(0,0), color=colours['4%'])
        hrc6 = plt.Line2D((0,1),(0,0), color=colours['6%'])
        hrc8 = plt.Line2D((0,1),(0,0), color=colours['8%'])
        hrc10 = plt.Line2D((0,1),(0,0), color=colours['10%'],linewidth=4)
        ax.legend([act1,mean1,hrc1,hrc2,hrc4,hrc6,hrc8,hrc10], ["Actual storage","Mean storage","1% HRC","2% HRC","4% HRC","6% HRC","8% HRC","10% HRC"])

    hrc_plotter(hrc,actual,means,ax1,'GWh',colours)
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)

#useful ASX hedgefunctions


def quart(year,q):
    '''Return period index given year and quarter'''
    return pd.Period(year = year, quarter = q,freq='Q-DEC')

def current_quarter(date=None):
    '''Given a datetime object, return the current quarter'''
    if date:
        return pd.period_range(date, date, freq='Q')[0]
    else:
        return pd.period_range(datetime.now().date(), datetime.now().date(), freq='Q')[0]

def hours_in_quarter(q):
    '''Calculate the number of hours in each quarter'''
    hours = ((q.end_time-q.start_time).days + 1)*24
    if q.quarter == 2: #if in second quater add an hour
        hours = hours + 1
    if q.quarter == 3: #if in third quarter subtract an hour
        hours = hours - 1
    return hours

def back_a_year(Q,year=1):
    now = datetime.now()
    return Q.start_time>(datetime(now.year-year,now.month,now.day))

def back_a_year_dt(dt,year=1):
    now = datetime.now()
    return dt>(datetime(now.year-year,now.month,now.day).date())

def CQ_data(spread_panel,daily_panel,price_data,quarter,ota_ben):
    '''Function to munge together useful daily time-series data for the current quarter;
           1. daily bid/ask spread data
           2. daily settlement data,
           3. daily mean actual spot price data,
           4. on above, calculate the implied average spot price for remainder of quarter.
    NOTE: this only works for 2012Q4 onwards as no spread data before that!'''
    CQ_beg = quarter.start_time - timedelta(days=7) #minus a week
    CQ_end = quarter.end_time + timedelta(days=7)   #plus a week, we'll crop this out later.
    CQ_days = round(hours_in_quarter(quarter)/24.0)

    def dtconvert(x): #simple date converter 
        date = x.split(' ')[0]
        time = x.split(' ')[1]
        return datetime(int(date.split('-')[0]),int(date.split('-')[1]),int(date.split('-')[2]),int(time[:2]),int(time[2:]))
    def remove_rouge_datetime(df): #as said...
        return df[df.index.map(lambda x: len(x)>14)]
    
    spread_panel.axes[1] = pd.period_range(spread_panel.axes[1][0], spread_panel.axes[1][-1], freq='Q') #quarterize...
    daily_panel.axes[1] = pd.period_range(daily_panel.axes[1][0], daily_panel.axes[1][-1], freq='Q')
    ask =spread_panel.ix[:,:,'Ask'].T
    bid =spread_panel.ix[:,:,'Bid'].T
    sett = daily_panel.ix[quarter.start_time.date():quarter.end_time.date(),quarter,'Sett'].T
    bid = remove_rouge_datetime(bid[quarter]) #remove rouge timestamp - don't know how that got there...
    ask = remove_rouge_datetime(ask[quarter])
    bid.index = bid.index.map(lambda x: dtconvert(x)) #convert dates to datetime object
    ask.index = ask.index.map(lambda x: dtconvert(x))
    CQ = pd.DataFrame({'CQ_bid':bid,'CQ_ask':ask}).dropna() #whack into df
    CQ = CQ.groupby(lambda x: x.date()).agg([('min','min'),('max','max')]) 
    CQ['CQ_Sett'] = sett 
    CQ['CQ_mean'] = price_data
    CQ['CQ_days'] = CQ.index.map(lambda x: x-quarter.start_time.date()+timedelta(days=1))
    if int(np.version.version.replace('.','')[0:3])<170:
        CQ['CQ_days'] = CQ['CQ_days'].map(lambda x: x.item().days)
    else:
        CQ['CQ_days'] = CQ['CQ_days'].map(lambda x: x/np.timedelta64(1,'D'))
    CQ['CQ_pc'] = CQ['CQ_days'].map(lambda x: x/CQ_days)
    CQ['CQ_imp'] = (CQ['CQ_Sett']-CQ['CQ_pc']*CQ['CQ_mean'])/(1-CQ['CQ_pc'])
    CQ = CQ.ix[CQ_beg.date():CQ_end.date(),]
    
    return CQ


#ASX plot functions

def forward_price_curve(figno,df,color_map,fig_file):
    '''Given an ASX future Panel, take only future quarters, 
       slice by the last date of each historic quarter, and plot'''

    def future(Q):
        return Q.end_time>datetime.now()
    
    def last_date_quarter(df):
        q = df.T.groupby([lambda x: x.year,lambda x: (x.month-1)//3 + 1])
        ldq = [q.groups[g][-1] for g in q.groups]
        return ldq

    forward = df.ix[:,:,'Sett']
    forward = forward[forward.index.map(lambda x: future(x))].T.dropna(thresh=2).T
    forward_ldq = forward.ix[:,last_date_quarter(forward)].T.sort().tail(8).T #last 2 years
    #Define the color map
    colors = np.r_[np.linspace(0.0, 1, num=len(forward_ldq.T.index)-1)] 
    cmap = plt.get_cmap(color_map)
    cmap_colors = cmap(colors)
    
    #plot
    plt.close(figno)
    fig = plt.figure(figno,figsize=[20,12])
    ax = fig.add_subplot(111)
    history_dfs = forward_ldq.ix[:,:-1]
    history_dfs = append_quarter(history_dfs)
    history_dfs.plot(drawstyle='steps-post',color=cmap_colors,ax=ax)
    yesterday_df = forward_ldq.ix[:,-1]
    yesterday_df = append_quarter(yesterday_df)
    yesterday_df.plot(drawstyle='steps-post',linewidth=4,color=cmap_colors[-1],ax=ax)
    ax.set_xlabel('')
    ax.set_ylabel('$/MWh')
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)

def bid_ask_plot(figno,df_ota,df_ben,fig_file):
    '''Plot current quarter trends'''
    fig = plt.figure(figno,figsize=[25,16])
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212, sharex=ax1)

    def bid_ask_sett_implied(df,ax,title,colour1,colour2,colour3,colour4):
        lw=3
        ms=20
        x = df.index
        bid_CQ_mins = df[('CQ_bid','min')].values
        ask_CQ_maxs = df[('CQ_ask','max')].values
        ax.fill_between(x,bid_CQ_mins,ask_CQ_maxs,color=colour2)
        df['CQ_Sett'].plot(marker='.',linewidth=0,markersize=20,ax=ax,color=colour1)
        df['CQ_mean'].plot(ax=ax,color=colour3,linewidth=lw)
        #We want to extent the implied price out to the end of the quarter, here we go...
        CQ_imp = df['CQ_imp'].dropna()
        CQ_imp = CQ_imp.append(CQ_imp.tail(1))
        newindex = CQ_imp.index.tolist()[:-1]
        newindex = newindex + [(current_quarter().end_time+timedelta(1)).date()]
        CQ_imp.index = newindex
        CQ_imp.plot(ax=ax,color=colour4,linewidth=lw)
        xlabels = ax.get_xticklabels() 
        for label in xlabels: 
            label.set_rotation(0) 
        ax.set_title(title)
        ax.set_xlabel('')
        ax.set_ylabel('$/MWh')
        ax.set_xlim([current_quarter().start_time,current_quarter().end_time])
        #Proxy artist legend
        set_leg = plt.Line2D(range(1), range(1), color="white", markersize=ms, marker='.', markerfacecolor=colour1)
        spd_leg = plt.Rectangle((0, 0), 1, 1, color=colour2)
        exm_leg = plt.Line2D((0,1),(0,0), color=colour3,linewidth=lw)
        imp_leg = plt.Line2D((0,1),(0,0), color=colour4,linewidth=lw)
        ax.legend([set_leg,spd_leg,exm_leg,imp_leg], ["Daily hedge settlement","Intraday Bid-Ask spread","Mean spot to date","Implied spot"])
        
    ota_colour1 = ea_s['or1']
    ota_colour2 = ea_s['or2']
    ota_colour3 = ea_s['rd1']
    ota_colour4 = ea_s['rd2']
    ben_colour1 = ea_s['bl1']
    ben_colour2 = ea_s['be1']
    ben_colour3 = ea_s['pp1']
    ben_colour4 = ea_s['pp2']

    bid_ask_sett_implied(df_ota,ax1,'Otahuhu',ota_colour1,ota_colour2,ota_colour3,ota_colour4)
    bid_ask_sett_implied(df_ben,ax2,'Benmore',ben_colour1,ben_colour2,ben_colour3,ben_colour4)
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)


def plot_monthly_volumes(figno,ota,ben,fig_file):
    '''Munge data from panel, and plot monthly trading volumes using a stacked bar plot'''
    ben_volumes = ben.ix[:,:,'Volume'].T.groupby([lambda x: x.year,lambda x: x.month]).sum()
    ota_volumes = ota.ix[:,:,'Volume'].T.groupby([lambda x: x.year,lambda x: x.month]).sum()
    hours = pd.Series(ben.axes[1].map(lambda x: hours_in_quarter(x)),index = ben.axes[1])

    func = lambda x: np.asarray(x) * np.asarray(hours) 
    ben_volumes_GWh = (ben_volumes.T.apply(func)/1000.0).T #to GWh
    ota_volumes_GWh = (ota_volumes.T.apply(func)/1000.0).T
    volumes_GWh= pd.DataFrame({'Otahuhu (GWh)':ota_volumes_GWh.sum(axis=1),'Benmore (GWh)':ben_volumes_GWh.sum(axis=1)})
    
    fig = plt.figure(figno,figsize=[20,10])
    ax = fig.add_subplot(111)
    v=volumes_GWh.plot(kind='bar',stacked=True,ax=ax,color=[ea_p['bl1'],ea_s['or1']])
    ax.set_xlabel('')
    ax.set_ylabel('GWh')
    #rint ax.get_xticklabels()
    newlabels=[]
    for xtl in ax.get_xticklabels():
        years = xtl.get_text()[1:-1].split(',')[0].replace(' ','')
        month = month1[int(xtl.get_text()[1:-1].split(',')[1].replace(' ',''))-1]
        newlabels.append(month + ', ' + years)
    ax.set_xticklabels(newlabels,fontsize=14)    
    v.grid(axis='x')
  
        
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)
 

def plot_open_interest(figno,ota,ben,fig_file):
    '''Munge data from panel, and plot daily open interest'''
    ben_opint = ben.ix[:,:,'Op Int']
    ota_opint = ota.ix[:,:,'Op Int']
    Q_hours = pd.Series({q: hours_in_quarter(q) for q in ben_opint.index}) #get hours in each quarter
    ota_opint_GWh = ((ota_opint.T.fillna(0.0)).dot(Q_hours)/1000.0) #dot multiply MW by hours, divide by 1000, to get GWh
    ben_opint_GWh = ((ben_opint.T.fillna(0.0)).dot(Q_hours)/1000.0)
    opint_GWh = pd.DataFrame({'Otahuhu':ota_opint_GWh,'Benmore':ben_opint_GWh})
    opint_GWh = opint_GWh[['Otahuhu','Benmore']].cumsum(axis=1) #swap columns and sum
    opint_GWh = opint_GWh[opint_GWh.index.map(lambda x: back_a_year_dt(x,year=2))]

    fig = plt.figure(figno,figsize=[20,12])
    ax = fig.add_subplot(111)
    ax.fill_between(opint_GWh.index, 0, opint_GWh['Benmore'],facecolor=ea_p['bl1'],label='Benmore')
    ax.fill_between(opint_GWh.index, 0, opint_GWh['Otahuhu'],facecolor=ea_s['or1'],label='Otahuhu')
    ax.set_xlabel('')
    ax.set_ylabel('GWh')
    tot = opint_GWh.tail(1).Benmore.values[0]
    otatot = opint_GWh.tail(1).Otahuhu.values[0]
    bentot = tot-otatot

    benleg = plt.Line2D((0,1),(0,0), color=ea_p['bl1'],linewidth=10)
    otaleg = plt.Line2D((0,1),(0,0), color=ea_s['or1'],linewidth=10)
    total_leg = plt.Line2D((0,0.1),(0,0), color='w',linewidth=10)

    ax.legend([benleg,otaleg,total_leg], ["Benmore (%i GWh)" % bentot,"Otahuhu (%i GWh)" % otatot,"Total = %i GWh" % tot],loc=2)
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)
    
def filter_last_year(df,CQ):
    '''Filter out the current past annual settlement data ready for plotting with plot_last_year'''
    forward_all = df.ix[:,:,'Sett']
    forward_all = forward_all[forward_all.index.map(lambda x: back_a_year(x))].T
    forward_year = forward_all.ix[forward_all.index.map(lambda x: x>(datetime.now().date()-timedelta(days=365))),:]
    summer = forward_year.ix[:,forward_year.columns.map(lambda x: x.quarter in [1,4])]
    winter = forward_year.ix[:,forward_year.columns.map(lambda x: x.quarter in [2,3])]
    
    summer_quart_past = summer.ix[:,summer.columns<CQ]
    summer_quart_futr = summer.ix[:,summer.columns>CQ]
    winter_quart_past = winter.ix[:,winter.columns<CQ]
    winter_quart_futr = winter.ix[:,winter.columns>CQ]
    
    if CQ.quarter in [1,4]: #if CQ is summer
        summer_quart_now = pd.DataFrame({CQ:summer[CQ]})
    else:
        summer_quart_now = None

    if CQ.quarter in [2,3]: #if CQ is winter
        winter_quart_now = pd.DataFrame({CQ:winter[CQ]})
    else:
        winter_quart_now = None
    
    summer = {'Past quarters':summer_quart_past,'Future quarters':summer_quart_futr,'Current quarter':summer_quart_now}
    winter = {'Past quarters':winter_quart_past,'Future quarters':winter_quart_futr,'Current quarter':winter_quart_now}

    return summer,winter

def plot_last_year(figno,df_dict_sum,df_dict_win,fig_file):
    '''Plots the last years worth of ASX data'''

    def subplotter(df_dict,ax,title):
        for i,v in df_dict.iteritems():
            if i == "Past quarters":
                if len(v.columns) > 1:
                    colors = np.r_[np.linspace(0.2, 1, num=len(v.columns))] 
                    cmap = plt.get_cmap("Blues")
                    blueshift = cmap(colors)       
                else:
                    blueshift = (0.81411766,0.88392158,0.94980392)
                v = append_day(v)
                v.plot(ax=ax,color=blueshift,drawstyle='steps-post')
            if i == "Future quarters":
                if len(v.columns) > 1:
                    colors = np.r_[np.linspace(0.2, 1, num=len(v.columns))] 
                    cmap = plt.get_cmap("Reds")
                    redshift = cmap(colors)[::-1]  
                else:
                    redshift = (0.99137255,0.79137256,0.70823531)
                v = append_day(v)
                v.plot(ax=ax,color=redshift,drawstyle='steps-post')
            if i == "Current quarter":
                if v is not None:
                    v = append_day(v)
                    v.plot(ax=ax,color=(0.03137255,0.1882353,0.41960785),drawstyle='steps-post')

        ax.set_ylabel('$/MWh') 
        ax.set_title(title)
        handles, labels = ax.get_legend_handles_labels()
        import operator
        hl = sorted(zip(handles, labels),key=operator.itemgetter(1))
        handles2, labels2 = zip(*hl)
        ax.legend(handles2, labels2,3)
        
    fig = plt.figure(figno,figsize=[20,30])
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)
    subplotter(df_dict_sum,ax1,'Summer quarters')
    subplotter(df_dict_win,ax2,'Winter quarters')
    plt.savefig(fig_file,bbox_inches='tight',transparent=True,pad_inches=0)

def asx_table_maker(otahuhu,benmore,ota,ben,CQ,tab_name):
    def table_generator(df_spread,df,CQ):
        '''Function to return useful stats on Hedge Market akin to Richard's table'''
        bid = df_spread.ix[:,CQ:,'Bid'].dropna(how='all',axis=1).ix[:,-1]
        offer = df_spread.ix[:,CQ:,'Ask'].dropna(how='all',axis=1).ix[:,-1]
        spread = 100*((offer-bid)/bid)
        Q_hours = pd.Series({q: hours_in_quarter(q) for q in bid.index}) #get hours in each quarter
        GWh = ((df.ix[-1,CQ:,'Op Int'].T.fillna(0.0))*(Q_hours)/1000.0)
        return pd.DataFrame({'Bid':bid,'Offer':offer,'Spread':spread,'UOI Futures (%)':100*GWh/GWh.sum()})
    
    bentab = table_generator(benmore,ben,CQ)
    otatab = table_generator(otahuhu,ota,CQ)
    table = pd.concat(dict(Otahuhu = otatab,Benmore = bentab),axis=1)
    f = open(tab_name,'w')
    f.write(table.to_latex(float_format='%.2f'))
    f.close


def plot_lwap(figno,lwaps,fig_name):
    plt.close(figno)
    fig = plt.figure(figno,figsize=[20,13])
    ax = fig.add_subplot(111)
    lwaps.plot(color=[ea_p['gy1'],ea_p['br2'],ea_p['rd1']],ax=ax)
    ax.set_ylabel('$/MWh') 
    plt.savefig(fig_name,bbox_inches='tight',transparent=True,pad_inches=0)

from bs4 import BeautifulSoup
import mechanize
class get_web_pics(object):
    '''Quick scrapper to download most recent Meridian snow storage pic and NIWA's forecast pic'''
    def __init__(self):
        super(get_web_pics, self).__init__()
        self.meridian_base_url = '''http://www.meridianenergy.co.nz/'''
        self.meridian_link = self.meridian_base_url + "about-us/generating-energy/lake-levels-and-snow-storage/snow-storage/"
        self.niwa_base_url = '''http://www.niwa.co.nz/'''
        self.niwa_link = self.niwa_base_url + "climate/sco"

    def get_snow_pic(self):
        try:
            self.br = mechanize.Browser()    # Browser
            html =self.br.open(self.meridian_link)
            soup = BeautifulSoup(html)
            image_tags = soup.findAll('img')
            #print image_tags

            for image in image_tags[0:1]:
                filename = image['src'].lstrip('http://')
                link = self.meridian_base_url + filename
            data = self.br.open(link).read()
            self.br.back()
            save = open('figures/snow.png', 'wb')
            save.write(data)
            save.close()            
            print "Successfully grabbed " + link        

        except:
            print "Unable to access " + self.meridian_link

    def get_niwa_pic(self):
        try:
            self.br = mechanize.Browser()    # Browser
            html =self.br.open(self.niwa_link)
            all_links = list(self.br.links())
            def get_climate_link(all_links):
                '''Attempt to grab the first climate related link, 
                   there must be a better way than this, but this will do for now...'''
                climate_links=[]
                for l in all_links:
                    if len(l.url.split('/'))>3:
                        if l.url.split('/')[1] == 'climate':
                            climate_links.append(l.url)
                return climate_links[0]
            most_recent_link = self.niwa_base_url + get_climate_link(all_links)
            #print most_recent_link
            html = self.br.open(most_recent_link)
            soup = BeautifulSoup(html)
            all_links = list(self.br.links())

            def get_latest_image(all_links):
                for l in all_links:
                    if "tcu_outlook" in l.url: #find link
                        return l.url
            image_url = get_latest_image(all_links)
            data = self.br.open(image_url).read() 
            self.br.back()
            save = open('figures/niwa.png', 'wb')
            save.write(data)
            save.close()    
            print "Successfully grabbed " + image_url        
            
        except:
            print "Unable to access " + self.niwa_link


if __name__ == '__main__':
    pass
