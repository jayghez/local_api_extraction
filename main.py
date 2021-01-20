
## Libraries 

import requests
import json
import zipfile
import os
from datetime import datetime
import pandas as pd
import snowflake.sqlalchemy 
from sqlalchemy import create_engine
from snowflake.sqlalchemy import ARRAY as Array,URL




now = datetime.now()
stamp = now.strftime("%m%d")


# Create Datadump 
def pull_data():
    
    
    
    tool_site = requests.get( 'endpoint',
      headers={"Authorization": " Autho API KEY" })

    collect = { 'tool_site_1': tool_site_1,
               'tool_site_2':tool_site_2, 'tool_site_3':tool_site_3, 'tool_site_4':tool_site_4}
    for x in collect:
        site = str(x)
        path_to = 'zip_drop/'+site+str(stamp) +'.zip' 
        open(path_to, 'wb').write(collect[x].content)
    return print(len(collect.keys()))

def extract_and_load():
    for filename in os.listdir('/zip_drop'):
        for subdir, dirs, files in os.walk('zip_drop'):
            for filename in files:
                filepath = subdir +'/' + filename
                if filename.endswith(".zip") and zipfile.is_zipfile(subdir +'/'+filename) == True :
                    with zipfile.ZipFile(subdir +'/'+filename, 'r') as zip_ref:
                        print(filepath)
                        zip_ref.extractall('csv_drop/'+filename.split('.')[0])
                else:
                    pass
    return print( 'Zips:', len(os.listdir('zip_drop')),
                'Folders ', len(os.listdir('csv_drop'))
                )




def pre_packing():
    for x in os.listdir('csv_drop'):
        if x.endswith(stamp):
            for csv in os.listdir('csv_drop/'+x):
                os.rename('csv_drop/'+x+'/'+csv ,'csv_drop/'+x[:-4]+'_'+csv )
    return print('Renamed' )


def to_snowflake(df,table,u,action):
    write_engine = create_engine(URL(
        user=u,
        authenticator='externalbrowser',
        account="acct",
        database = 'db',
        role='user',
        schema='name',
        numpy=True
    ))
    connection = write_engine.connect()
    schema = 'name'
    try:
        if action=='replace':
            #connection.execute("DROP TABLE ANALYTICS."+schema+'.'+table)
            df.to_sql(table, con=write_engine, index=False, if_exists='append')
            connection.execute("commit;")
        if len(df) > 15000:
            n_chunks = (len(df)//15000) + 1
            dfs = np.array_split(df,n_chunks)
            for df in dfs:
                df.to_sql(table, con=write_engine, index=False, if_exists='append')
                connection.execute("commit;")
        else:
            df.to_sql(table, con=write_engine, index=False, if_exists='append')
            connection.execute("commit;")
    finally:
        connection.close()
        write_engine.dispose()



def table_splitter(u):

    
    drop = 'csv_drop'
    for x in os.listdir(drop)[50:]:
        if x.endswith('.csv') and ('versions') not in x and 'attempts' not in x and ('question_responses') not in x and 'rigup_learning_items' not in x:
            if x.startswith('name'):
                df =pd.read_csv(drop+'/'+x)
                x = x[:-4]
                to_snowflake(df,x,u,'replace')
                print(x)
            if x.startswith('name'):
                df =pd.read_csv(drop+'/'+x)
                x = x[:-4]
                to_snowflake(df,x,u,'replace')
                print(x)
            if x.startswith('name'):
                df =pd.read_csv(drop+'/'+x)
                x = x[:-4]
                to_snowflake(df,x,u,'replace')
                print(x)
            if x.startswith('name'):
                df =pd.read_csv(drop+'/'+x)
                x = x[:-4]
                to_snowflake(df,x,u,'replace')
                print(x)
            else:
                pass


    return 'done'
            
       
u='u' 
#pull_data()
#extract_and_load()
#pre_packing()
table_splitter(u)
