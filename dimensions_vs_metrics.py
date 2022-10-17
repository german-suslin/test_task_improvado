import pandas as pd
import numpy as np
import re
filename = 'Data_Dictionary_for_CSV_2022_10_11.csv'

df = pd.read_csv(filename)

# take
df_count = df.groupby('sql_field_name').size().reset_index(name='count')
df_sorted_count = df_count.sort_values('count', ascending=False)


unique = pd.DataFrame(df['sql_field_name'].unique(), columns=['sql_field_name'])
df_unique = unique.copy()
print('unique', df_unique.shape)
# print('type', df_unique.columns)
df_id_end = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'\W*_+id_*\W*')]
# print('type', df_id_end.columns)

# print(df_unique.columns, df_id_end.columns)
df_sorted_count = df_sorted_count.merge(df_id_end,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')

df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - id end', df_sorted_count.shape)
print('shape', df_sorted_count.head())
# print('unique - id', df_unique.columns)

# df_unique = pd.merge(df_unique, df_id_end, on=['sql_field_name'], how="outer")
# pd.merge(dfA, dfB, on=['a','b'], how="outer", indicator=True
#               ).query('_merge=="left_only"')
# df_unique = df_unique[df_unique['_merge'] == 'left_only']
# df_unique = pd.merge(df, unique, on='sql_field_name', how = 'left')
# df = pd.merge(dfA, dfB, on=['a','b'], how="outer", indicator=True)
# df = df[df['_merge'] == 'left_only']

df_id = df_unique[df_unique['sql_field_name'].str.contains(r'\W*_+id_*\W*|\W*_*id_+\W*')]


df_id_begin = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'\Aid')]

df_sorted_count = pd.merge(df_sorted_count, df_id_begin,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - id begin', df_sorted_count.shape)

df_date = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'date\b')]
df_sorted_count = pd.merge(df_sorted_count, df_date,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - date', df_sorted_count.shape)

df_campain = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'campaign\b|name\b')]
df_sorted_count = pd.merge(df_sorted_count, df_campain,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - campain&name', df_sorted_count.shape)

df_city = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'city\b')]
df_sorted_count = pd.merge(df_sorted_count, df_city,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - city', df_sorted_count.shape)

df_clicks = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'clicks\b')]
df_sorted_count = pd.merge(df_sorted_count, df_clicks,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - clicks', df_sorted_count.shape)

df_impressions = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'impressions\b')]
df_sorted_count = pd.merge(df_sorted_count, df_impressions,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - impressions', df_sorted_count.shape)

df_spend = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'spend\b')]
df_sorted_count = pd.merge(df_sorted_count, df_spend,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - spend', df_sorted_count.shape)

df_conversions = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'conversions\b')]
df_sorted_count = pd.merge(df_sorted_count, df_conversions,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - conversions', df_sorted_count.shape)

df_country = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'country\b')]
df_sorted_count = pd.merge(df_sorted_count, df_country,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - country', df_sorted_count.shape)

df_cost = df_sorted_count[df_sorted_count['sql_field_name'].str.contains(r'cost\b')]
df_sorted_count = pd.merge(df_sorted_count, df_cost,
                     on=['sql_field_name', 'count'],
                     how="outer", indicator=True).query('_merge=="left_only"')
df_sorted_count.drop(columns = ['_merge'], inplace=True)
print('unique - cost', df_sorted_count.shape)

print('impressions', df_impressions.shape)
print('clicks', df_clicks.shape)
print('city', df_city.shape)
print('campain', df_campain.shape)
# df_date = df_unique[df_unique[0].str.contains(r'\W*_+date_*\W*|\W*_*date_+\W*|date')]
print('id', df_id.shape)
print('id end', df_id_end.shape)
print('id begin', df_id_begin.shape)
print('date', df_date.shape)
print(sum([df_id_end.shape[0], df_id_begin.shape[0]]))
# print(df_unique.columns)
# df_click = df['sql_field_name'][df['sql_field_name'].str.contains(r'\W*\w*click\W*\w*')]
# df_date =
# df_id_in_click = df_click[df_click.str.contains(r'\W*_+id_*\W*|\W*_*id_+\W*')]
# for i in range(len(df_id)//10):
#     print(df_id[i:10+i])
lists = np.array(df_cost)
for i in lists:
    print(i)
lists = np.array(df_sorted_count)
amount = 0
counter = 0
print(df_sorted_count.head())
for i in lists:
    # print(i)
    # if amount < 0.6*df.shape[0]:
    amount += i[1]
    counter += 1
    # else:
    #     break
print('amount', amount)
print('percent', amount/df.shape[0]*100)
# print('impressions', df_impressions.shape)
# print('clicks', df_clicks.shape)
# print('city', df_city.shape)
# print('campain', df_campain.shape)
# print('id', df_id.shape)
# print('id end', df_id_end.shape)
# print('id begin', df_id_begin.shape)
# print('date', df_date.shape)
# counter = 0
# for i in lists:
