# -*- coding: utf-8 -*-
"""
Created on Thu Jan  4 13:56:09 2024

@author: KumarAkashdeep
"""

#trying the working codebase without set_ index



import pandas as pd
import numpy as np

pool2curve = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\pool_to_curve_mapping.csv')
pool2curve = pool2curve[['pool_name','pool_id','product_group','base_ftp_curve','constant_parameter_value']]

pool2curve.rename(columns = {'base_ftp_curve' : 'curve_name'}, inplace=True)


ftpSynthetic = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_synthetic_curve.csv')
ftpSynthetic = ftpSynthetic[['curve_components','rate','curve_name']]

ftpSynthetic.rename(columns = {'rate' : 'base_ftp_rate'}, inplace=True)


df = pool2curve.merge(ftpSynthetic,on = 'curve_name', how ='left')
pd.set_option('display.max_rows', None)



ftpComponents = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_curve_components.csv')
ftpComponents =  ftpComponents[['tenor_value','tenor_unit','curve_components']]


df = df.merge(ftpComponents,on= 'curve_components',how ='left')

df.sort_values('curve_name', inplace=True)
pd.set_option('display.max_rows', None)
 

casaRate = pd.read_csv(('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\casa_constant_rates.csv'))
casaRate= casaRate[['custom_rate','constant_rate_parameter']]


casaRate.rename(columns = {'constant_rate_parameter' : 'constant_parameter_value'},inplace =  True )

#df1_filtered = df1[df1['curve_name'] == 'constant parameter'].copy(deep=True)

# Assuming df1 and df2 are your two dataframes
df_filtered = df[df['curve_name'] == 'Constant Parameter'].copy(deep = True)

"""
df_filtered['mergebck'] = df_filtered.index


# Set 'constant_parameter_value' as the index of casaRate
#casaRate.set_index('constant_parameter_value', inplace=True)

# Now merge df_filteredand casaRate on the index
merged_df = df_filtered.merge(casaRate, on = 'constant_parameter_value', how='left')


# Transfer the values
merged_df['base_ftp_rate'] = merged_df['custom_rate']

# Drop the 'custom_rate' column
merged_df = merged_df.drop('custom_rate', axis=1)
merged_df.set_index('mergebck',inplace =True )
merged_df.rename(columns = {'mergebck' : 'Index'},inplace = True)

df.update(merged_df)
"""


df_filtered = df[df['curve_name'] == 'Constant Parameter'].copy(deep = True)

# Save the original index for later use
original_index = df_filtered.index

# Now merge df_filtered and casaRate on the 'constant_parameter_value'
merged_df = df_filtered.merge(casaRate, on = 'constant_parameter_value', how='left')

# Transfer the values
merged_df['base_ftp_rate'] = merged_df['custom_rate']

# Drop the 'custom_rate' column
merged_df = merged_df.drop('custom_rate', axis=1)

# Use the original index to update the original DataFrame
for i in original_index:
    if i in merged_df.index:
        df.loc[i] = merged_df.loc[i]




df = df.drop(['constant_parameter_value'], axis=1)



credliq = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\cred_liq.csv')
credliq = credliq[['pool_name','pool_id','product_group','tenor_value','tenor_unit','Liquidity_Premium_Rate','Credit_Spread_Rate']]

df['tenor_value'] = df['tenor_value'].astype(float)
credliq['tenor_value'] = credliq['tenor_value'].astype(float)

df_credliq =  df.merge(credliq, on= ['pool_id','pool_name','product_group','tenor_value','tenor_unit'],how = 'left')



drop_unwanted = df_credliq.drop(['curve_name','curve_components'],axis =1)

format_ans =  drop_unwanted.copy()

format_ans['Credit_Spread_Rate'].fillna(0, inplace=True)
format_ans['Liquidity_Premium_Rate'].fillna(0, inplace=True)
format_ans['final_ftp'] = format_ans['base_ftp_rate'] + format_ans['Credit_Spread_Rate'] + format_ans['Liquidity_Premium_Rate']




#sorting the answer in desired format

format_ans ['tenor_unit_num'] = format_ans['tenor_unit'].map({'M': 1, 'Y': 12})
format_ans['tenor_in_months'] = format_ans['tenor_value'] * format_ans['tenor_unit_num']
format_ans = format_ans.sort_values('tenor_in_months')

# If you want to drop the 'tenor_unit_num' and 'tenor_in_months' columns after sorting
format_ans = format_ans.drop(['tenor_unit_num', 'tenor_in_months'], axis=1)


order = format_ans[['pool_id','pool_name','product_group','tenor_value','tenor_unit','base_ftp_rate','Credit_Spread_Rate','Liquidity_Premium_Rate','final_ftp']]


#order.to_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\order.csv', index=False)
