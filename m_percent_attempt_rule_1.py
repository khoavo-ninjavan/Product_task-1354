import glob
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
pd.set_option('display.max_columns', None)

list_call_log = glob.glob(r'D:\rerun_5\data\pdt\alo2_call*.pq')
list_event = glob.glob(r'D:\rerun_5\data\pdt\deli_*.pq')

raw = {}
for i in list_event:
    for a in list_call_log:
        if i[31:33] == a[34:36]:
            raw[i] = a

# medium calculate
soon_attempt = 0
late_attempt = 0
total_attempt = 0
loop = 0
raw_attempt = pd.DataFrame()

for event,call_log in raw.items():
    print(event)
    print(call_log)
    d = pd.read_parquet(event)
    c = pd.read_parquet(call_log)
    
    # clean attempt
    d = d[['driver_id','route_id','callee','attempt_datetime','attempt_sync_at']]
    d.attempt_sync_at = d.attempt_sync_at.str[:19].astype('datetime64[ns]')
    d.attempt_datetime = d.attempt_datetime.str[:19].astype('datetime64[ns]')
    d['callee'] = d['callee'].str.replace(' ','').str[-9:]
    d = d.drop_duplicates()
    
    d['attempt_sync_at'] = d.groupby(['driver_id','route_id','callee'])['attempt_sync_at'].transform('max')
    d = d.groupby(['driver_id','route_id','callee','attempt_sync_at'], as_index = False).agg({'attempt_datetime':'max'})
    d.sort_values(by =  'attempt_datetime', ascending = True, inplace = True)
    d['last_attempt_datetime'] = d.groupby('callee').attempt_datetime.shift(1)

    full = d.merge(c, on = 'callee', how = 'left')
    full = full[full.attempt_datetime >= full.started_at]
    full.drop(full[(full.last_attempt_datetime.isna()== False) & (full.started_at < full.last_attempt_datetime)].index, inplace = True)

    full['att_sync_second'] = (full.attempt_sync_at.dt.hour)*3600 + (full.attempt_sync_at.dt.minute)*60 + (full.attempt_sync_at.dt.second)
    full['sync_second'] = (full.sync_at.dt.hour)*3600 + (full.sync_at.dt.minute)*60 + (full.sync_at.dt.second)
    full = full[['attempt_datetime','att_sync_second','sync_second']]
    full['sync_diff'] = full['att_sync_second'] - full['sync_second']
    full['is_late'] = full.apply(lambda x: 1 if x.sync_diff < 0 else 0, axis = 1)
    full['is_att_sync_soon'] = full.assign(is_att_sync_soon = np.where(full.is_late == 1,0,1)).groupby('attempt_datetime')['is_att_sync_soon'].transform('max')
    full = full[['attempt_datetime','is_att_sync_soon']].drop_duplicates()
    #part_attempt = full[['sync_diff']]

    #if loop != 0:
    raw_attempt = pd.concat([raw_attempt,full])
    soon_attempt += len(full[full.is_att_sync_soon == 1])
    late_attempt += len(full[full.is_att_sync_soon == 0])
    total_attempt += len(full)

    

