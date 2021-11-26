'''this script create  integration to https://randomuser.me/ API,
 and create diffrent datasets and place them in MySQL DB
 '''

__version__ = '0.1'
__author__ = 'Michal Sheinberg'

import requests
import pandas as pd
from sqlalchemy import create_engine
import configparser


# getting config info
config = configparser.ConfigParser()
config.read('config.json.txt')
query_params= config.get('api_conection','query_params')
host=config.get('mysql_conection', 'host')
user=config.get('mysql_conection', 'user')
password=config.get('mysql_conection', 'passwd')
db=config.get('mysql_conection', 'db')

#integration with https://randomuser.me/ API
response=requests.get('https://randomuser.me/api/'+query_params)

#Create dataset of 4500 users
x=response.json()
df=pd.json_normalize(x['results'])

#data cleaning- rename & data types
df.columns = df.columns.str.replace('.', '_',regex=True)
df['registered_date'] = pd.to_datetime(df['registered_date'],format='%Y-%m-%d %H:%M:%S')

#MySQL DB connection
engine = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/' + db)

#Split the dataset to 2 gender datasets(male, female)
male_df, female_df = df.loc[df['gender'] =='male'] ,df.loc[df['gender'] =='female']

# store each one of the datasets in separated mysql table named “YOUR_NAME_test_male/female”
female_df.to_sql(name='Michal_test_female',con=engine,if_exists='replace')
male_df.to_sql(name='Michal_test_male',con=engine,if_exists='replace')

#Split the dataset to 10 subsets by dob.age column in groups of 10 (10s 20s 30s etc.)
#Store each one of subsets in “YOUR_NAME_test_{subset_number} in mysql

group_dict = {}
grouped = df.groupby(df['dob_age'] // 10)
for age, group in grouped:
    group.to_sql(name='Michal_test_'+str(age), con=engine, if_exists='replace')
    group_dict[age] = group

#Write a sql query that will return the top 20 last registered males and females form and save it as YOUR_NAME_test_20
sql_top_20 = 'select * from (select * from Michal_test_female ' \
             'union select * from Michal_test_male ) as All_user ' \
             'order by registered_date desc  limit 20 '
pd.read_sql(sql_top_20,engine).drop('index',axis=1).to_sql(name='Michal_test_20', con=engine, if_exists='replace')

#Create a dataset that combines data from YOUR_NAME_test_20 and data from YOUR_NAME_test_5 table
#Make sure each row presented only once and there is no multiplication of data.

sql_top_20_age_5 = 'select * from Michal_test_20 ' \
             'UNION ' \
             'select * from Michal_test_5 '
df_20_age_5= pd.read_sql(sql_top_20_age_5,engine)

# same as above but with  concat
Michal_test_20=pd.read_sql('select * from Michal_test_20 ',engine)
df_20_age_5v2=pd.concat([group_dict[5],Michal_test_20]).drop_duplicates().reset_index(drop=True)

#Create json from the mentioned dataset and store it locally as first.json

df_20_age_5.to_json('first.json', orient = 'records')

#Create a dataset that combines data from YOUR_NAME_test_20 and data from
#YOUR_NAME_test2 table. In case the same rows are presented in 2 datasets both ofrows supposed to be presented.

sql_top_20_age_2 = 'select * from Michal_test_20 ' \
             'UNION ALL' \
             ' select * from Michal_test_2'

df_20_age_2= pd.read_sql(sql_top_20_age_2,engine)
# using concat
df_20_age_2v2= pd.concat([group_dict[2], Michal_test_20])

# Create json from the mentioned dataset and store it locally as second.json
df_20_age_5.to_json('second.json', orient = 'records')