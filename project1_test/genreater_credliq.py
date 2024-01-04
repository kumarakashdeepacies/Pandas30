# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 14:07:02 2024

@author: KumarAkashdeep
"""


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


df_filtered['mergebck'] = df_filtered.index


# Set 'constant_parameter_value' as the index of casaRate
#casaRate.set_index('constant_parameter_value', inplace=True)

# Now merge df_filtered and casaRate on the index
merged_df = df_filtered.merge(casaRate, on = 'constant_parameter_value', how='left')


# Transfer the values
merged_df['base_ftp_rate'] = merged_df['custom_rate']

# Drop the 'custom_rate' column
merged_df = merged_df.drop('custom_rate', axis=1)
merged_df.set_index('mergebck',inplace =True )
merged_df.rename(columns = {'mergebck' : 'Index'},inplace = True)

df.update(merged_df)

df = df.drop(['constant_parameter_value'], axis=1)


#@###########################################################################




dff  = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\pool_wise_spread_mapping.csv')

# Assuming 'df' is your DataFrame
#df['spread_value'] = df.apply(lambda row: row['spread_name'] if row['spread_type'] == 'Credit spread' else row['constant_parameter_value'], axis=1)
#df['liquidity_premium'] = df.apply(lambda row: row['spread_name'] if row['spread_type'] == 'Liquidity premium' else row['constant_parameter_value'], axis=1)

# Drop unnecessary columns
dff = dff.drop(columns=['spread_name', 'configuration_date'])

# Remove duplicates
#df = df.drop_duplicates()


# Assuming 'df' is your DataFrame
credit_spread = dff[dff['spread_type'] == 'Credit spread']
liquidity_premium = dff[dff['spread_type'] == 'Liquidity premium']

credit_spread.rename(columns = {'spread_type' : 'cre_spd_present','spread_source' : 'cre_spd_src'},inplace = True)
liquidity_premium.rename(columns = {'spread_type' : 'liq_pre_present','spread_source' : 'liq_pre_src'},inplace = True)


# Assuming 'df' is your DataFrame
#liquidity_premium['new_column'] = liquidity_premium.apply(lambda row: row['spread_curve'] if row['spread_curve'] != 'N.A.' else row['constant_parameter_value'], axis=1)


# Assuming 'df' is your original DataFrame
liquidity_premium1 = liquidity_premium.copy()
# Now you can safely create a new column
liquidity_premium1.loc[:, 'new_column'] = liquidity_premium1.apply(lambda row: row['spread_curve'] if row['spread_curve'] != 'N.A.' else row['constant_parameter_value'], axis=1)
liquidity_premium1 = liquidity_premium1.drop(columns=['spread_curve', 'constant_parameter_value'])
liquidity_premium1.rename(columns = {'new_column' : 'liq_pre_pckUp'},inplace = True)



credit_spread1 = credit_spread.copy()
# Now you can safely create a new column
credit_spread1.loc[:, 'new_column'] = credit_spread1.apply(lambda row: row['spread_curve'] if row['spread_curve'] != 'N.A.' else row['constant_parameter_value'], axis=1)
credit_spread1 = credit_spread1.drop(columns=['spread_curve', 'constant_parameter_value'])
credit_spread1.rename(columns = {'new_column' : 'cre_spd_pckUp'},inplace = True)



credit_spread1 = credit_spread1.drop(columns=['entity','entity_level_pool'])
liquidity_premium1 = liquidity_premium1.drop(columns=['entity','entity_level_pool'])





casaRate = pd.read_csv(('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\casa_constant_rates.csv'))
liqcasa = casaRate[['custom_rate','constant_rate_parameter']]
creditcasa = casaRate[['custom_rate','constant_rate_parameter']]


creditcasa.rename(columns = {'custom_rate' :  'Credit_Spread_Rate', 'constant_rate_parameter' :'cre_spd_pckUp'},inplace =True)
liqcasa.rename(columns = {'custom_rate' :  'Liquidity_Premium_Rate', 'constant_rate_parameter' :'liq_pre_pckUp'},inplace =True)


credit_spread1 =  credit_spread1.merge(creditcasa,on = 'cre_spd_pckUp',how = 'left')
liquidity_premium1 =  liquidity_premium1.merge(liqcasa,on = 'liq_pre_pckUp',how = 'left')






# the logic is to find the final casa value for matruing assets 








synthetic = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_synthetic_curve.csv')
synthetic =  synthetic[['curve_components','curve_name','rate']]



synthetic.rename(columns = {'curve_name' : 'cre_spd_pckUp'},inplace = True )
credit_spread1 = credit_spread1.merge(synthetic,on = 'cre_spd_pckUp', how ='left')
credit_spread1.loc[np.isnan(credit_spread1['Credit_Spread_Rate']), 'Credit_Spread_Rate'] = credit_spread1['rate']
credit_spread1 = credit_spread1.drop(columns=['rate'])








synthetic2 = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_synthetic_curve.csv')
synthetic2 =  synthetic2[['curve_components','curve_name','rate']]

synthetic2.rename(columns = { 'curve_name' : 'liq_pre_pckUp'},inplace = True )
liquidity_premium1 = liquidity_premium1.merge(synthetic2,on = 'liq_pre_pckUp', how ='left')
liquidity_premium1.loc[np.isnan(liquidity_premium1['Liquidity_Premium_Rate']), 'Liquidity_Premium_Rate'] = liquidity_premium1['rate']
liquidity_premium1 = liquidity_premium1.drop(columns=['rate'])



ftpComponents1 = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_curve_components.csv')
ftpComponents1 =  ftpComponents1[['tenor_value','tenor_unit','curve_components']]
credit_spread1 = credit_spread1.merge(ftpComponents1,on = 'curve_components', how ='left')

ftpComponents2 = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_curve_components.csv')
ftpComponents2 =  ftpComponents2[['tenor_value','tenor_unit','curve_components']]
liquidity_premium1 = liquidity_premium1.merge(ftpComponents2,on = 'curve_components', how ='left')


after_credit =  credit_spread1.copy()
after_premium = liquidity_premium1.copy()




credliq =liquidity_premium1.merge(credit_spread1,on= ['pool_id','pool_name','product_group','tenor_value','tenor_unit'],how = 'outer')
credliq['curve_components_x'].fillna(credliq['curve_components_y'], inplace=True)

credliq_format =  credliq[['pool_id','pool_name','tenor_value','tenor_unit','Liquidity_Premium_Rate','Credit_Spread_Rate']]
#credliq.rename(columns = {'curve_components_y' : 'curve_components'},inplace  = True )


#credliq.to_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\cred_liq.csv', index=False)

answer = credliq_format.merge(df,on= ['pool_name','pool_id','tenor_value','tenor_unit'], how = 'outer')


format_ans = answer[['pool_name','pool_id','product_group','tenor_value', 'tenor_unit','base_ftp_rate','Credit_Spread_Rate','Liquidity_Premium_Rate']]


# Assuming 'df' is your DataFrame
format_ans = format_ans[~((format_ans['pool_name'] == 'Maturing deposits') & (format_ans['Credit_Spread_Rate'].isna()) & (format_ans['Liquidity_Premium_Rate'].isna()))]


format_ans.dropna(subset=['tenor_value', 'tenor_unit'], how='all', inplace=True)



format_ans ['tenor_unit_num'] = format_ans['tenor_unit'].map({'M': 1, 'Y': 12})
format_ans['tenor_in_months'] = format_ans['tenor_value'] * format_ans['tenor_unit_num']
format_ans = format_ans.sort_values('tenor_in_months')

# If you want to drop the 'tenor_unit_num' and 'tenor_in_months' columns after sorting
format_ans = format_ans.drop(['tenor_unit_num', 'tenor_in_months'], axis=1)



format_ans['Credit_Spread_Rate'].fillna(0, inplace=True)
format_ans['Liquidity_Premium_Rate'].fillna(0, inplace=True)

format_ans['final_ftp'] = format_ans['base_ftp_rate'] - format_ans['Credit_Spread_Rate'] - format_ans['Liquidity_Premium_Rate']

#format.drop_duplicates(keep='last')


#format['final_ftp'] = format['base_ftp_rate'] - format['Credit_Spread_Rate']  -  format['Liquidity_Premium_Rate']