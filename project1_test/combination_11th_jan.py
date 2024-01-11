# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 10:15:37 2024

@author: KumarAkashdeep
"""


import pandas as pd
import numpy as np

casa_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\casa_constant_rates.csv', na_values=['-'])
curve_repo_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\curve_repository_mul.csv',  na_values=['-'])
ftp_synthetic_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_synthetic_curve.csv', na_values=['-'])
ftp_components_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\ftp_curve_components.csv' , na_values=['-'])
#pool2spread_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\pool_wise_spread_mapping.csv', na_values=['-'])
pool2spread_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\pool_wise_spread_mapping.csv', na_values=['-'])
pool2curve_org = pd.read_csv('C:\\Users\\KumarAkashdeep\\OneDrive - ACIES\\Desktop\\project1\\FTP Summarised view\\pool_to_curve_mapping.csv', na_values=['-'])




# Refactoring the user's code to improve the conditional merge and handle new spread types and sources dynamically

# Step 1: Base FTP calculation
pool2curve = pool2curve_org[['pool_name', 'product_group', 'base_ftp_curve', 'constant_parameter_value', 'pool_id']]
ftp_synthetic = ftp_synthetic_org[['curve_components', 'curve_name', 'rate']]
pool2curve.rename(columns={'base_ftp_curve': 'curve_name'}, inplace=True)
curve2synthetic = pool2curve.merge(ftp_synthetic, on='curve_name', how='left')
ftp_components = ftp_components_org[['tenor_value', 'tenor_unit', 'curve_components']]
curve2synthetic2components = curve2synthetic.merge(ftp_components, on='curve_components', how='left')
curve2synthetic2components.rename(columns={'rate': 'base_ftp_rate'}, inplace=True)

filtered_base_casa = curve2synthetic2components[curve2synthetic2components['curve_name'] == 'Constant Parameter']
casa = casa_org[['constant_rate_parameter', 'custom_rate']]
casa.rename(columns = {'constant_rate_parameter' : 'constant_parameter_value'},inplace = True)
filtered_base_casa = filtered_base_casa.merge(casa, on = 'constant_parameter_value', how = 'left')
filtered_base_casa = filtered_base_casa[['pool_name','product_group','pool_id','tenor_unit','tenor_value','custom_rate']]
filtered_base_casa.rename(columns =  {'custom_rate' : 'base_ftp_rate'},inplace = True)


base_ftp_check = curve2synthetic2components[['pool_id', 'pool_name', 'product_group', 'tenor_value', 'tenor_unit', 'base_ftp_rate']]
base_ftp_check = base_ftp_check.merge(filtered_base_casa, on = ['pool_id', 'pool_name', 'product_group', 'tenor_value', 'tenor_unit', 'base_ftp_rate'], how = 'left')

# Step 2: Credit spread and credit premium calculation
c2s2c = base_ftp_check.copy()
pool2spread = pool2spread_org[['pool_id', 'pool_name', 'product_group', 'spread_type', 'spread_source', 'spread_curve', 'spread_name', 'constant_parameter_value']]
pool2spread = pool2spread.dropna(subset=['spread_source'])
c2s2c = c2s2c.merge(pool2spread, on=['pool_id', 'pool_name', 'product_group'], how='right')
#c2s2c.drop(['curve_name', 'constant_parameter_value_x'], axis=1, inplace=True)
c2s2c.rename(columns={'constant_parameter_value': 'Constant Parameter', 'spread_curve': 'Curve Based'}, inplace=True)

# Identifying unique spread types and sources
unique_spread_types = pool2spread['spread_type'].unique()
unique_spread_sources = pool2spread['spread_source'].unique()

# Prepare source_to_df dictionary
casa = casa_org[['constant_rate_parameter', 'custom_rate']]
casa.rename(columns={'custom_rate': 'rate', 'constant_rate_parameter': 'Constant Parameter'}, inplace=True)
ftp_synthetic_casa = ftp_synthetic.copy()
ftp_synthetic_casa = ftp_synthetic.merge(ftp_components, on='curve_components', how='right')
ftp_synthetic_casa.rename(columns={'curve_name': 'Curve Based'}, inplace=True)


source_to_df = {
    'Curve Based': ftp_synthetic_casa,
    'Constant Parameter': casa
}

# Initialize an empty DataFrame for the final result
final_result = pd.DataFrame()

##################################

# Iterate over each unique spread type and source
for spread_type in unique_spread_types:
    for spread_source in unique_spread_sources:
        # Filter liability based on spread type and source
        liability = c2s2c[(c2s2c['spread_type'] == spread_type) & (c2s2c['spread_source'] == spread_source)]

        # Determine the key for merging based on spread source
        merge_key = [spread_source]
        if spread_source == 'Curve Based':
            merge_key += ['tenor_value', 'tenor_unit']

        # Debugging: Print merge keys to verify
        print(f"Merge keys for {spread_type}, {spread_source}: {merge_key}")

        # Ensure the merge keys exist in both dataframes
        if not all(k in liability.columns for k in merge_key) or not all(k in source_to_df[spread_source].columns for k in merge_key):
            print(f"Merge key columns not found in both dataframes for {spread_type}, {spread_source}")
            continue


        # Perform the merge operation
        try:
            merged_df = pd.merge(liability, source_to_df[spread_source], left_on=merge_key, right_on=merge_key, how='left')
        except KeyError as e:
            #print(f"KeyError during merge for {spread_type}, {spread_source}: {e}")
            continue


        # Add spread type as a new column and store the rate
        merged_df[spread_type] = merged_df['rate']

        # Debugging: Print the contents of merged_df before appending
        #print(f"Merged DataFrame for {spread_type}, {spread_source}:\n", merged_df)
        
        if not isinstance(final_result, pd.DataFrame):
            #print(f"Error: final_result is not a DataFrame. It's a {type(final_result)}")
    # Optionally, reinitialize final_result if it's not a DataFrame
            final_result = pd.DataFrame()

# Append operation
        final_result = pd.concat([final_result, merged_df], ignore_index=True)
#        final_result = final_result.append(merged_df)

        # Append the processed DataFrame to the final result
        #final_result = final_result.append(merged_df)

# Debugging: Print the final result after all appends
#print("Final result DataFrame after all appends:\n", final_result)

final_result = final_result.drop(['spread_source','Curve Based','curve_components','spread_type', 'spread_name', 'Constant Parameter', 'rate'], axis=1)

        
# Step 3: Post-processing the final result
final_result = final_result.groupby(['pool_id', 'pool_name', 'tenor_value', 'tenor_unit', 'base_ftp_rate']).max().reset_index()
final_result = final_result[['pool_id', 'pool_name', 'tenor_value', 'tenor_unit', 'base_ftp_rate'] + list(unique_spread_types)]

#final_result = final_result[~final_result[list(unique_spread_types)].isna().all(axis=1)]

"""
#final_result.dropna(subset=['tenor_value', 'tenor_unit'], how='all', inplace=True)
final_result['tenor_unit_num'] = final_result['tenor_unit'].map({'M': 1, 'Y': 12})
final_result['tenor_in_months'] = final_result['tenor_value'] * final_result['tenor_unit_num']
final_result = final_result.sort_values('tenor_in_months').drop(['tenor_unit_num', 'tenor_in_months'], axis=1)
"""


final_result['Credit spread'].fillna(0, inplace=True)
final_result['Liquidity premium'].fillna(0, inplace=True)

final_result['final_ftp'] = final_result['base_ftp_rate'] + final_result['Credit spread'] + final_result['Liquidity premium']

#this output just have liability in it not the asset 

output_data = final_result



