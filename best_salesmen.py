import pandas as pd
import numpy as np
import re
filename = 'best_salesman_homework.csv'

df = pd.read_csv(filename)

# get data by deals and first touch
deals = df[df['event_name'] == 'deal']
first_touch = df[df['event_name'] == 'first_touch']

# count first touches and deals for every manager
grouped_deals = deals.groupby('manager_nickname').size().reset_index(name='count')
all_deals = first_touch.groupby('manager_nickname').size().reset_index(name='count')

# join tables for every manager and find ratio between first touch and deals
df2 = pd.merge(all_deals, grouped_deals, on=['manager_nickname'], how='inner')
df2['ratio'] = df2['count_y']/df2['count_x']*100

print(df2[['manager_nickname', 'ratio']])