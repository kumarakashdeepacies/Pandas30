import pandas as pd

def second_highest_salary(employee: pd.DataFrame) -> pd.DataFrame:
    df = employee.sort_values(by = ('salary'),ascending = False)
    df = df.rename(columns = {'salary' : 'SecondHighestSalary'})
    df = df.drop_duplicates(subset = 'SecondHighestSalary')
    if len(df) >= 2:
        return df[['SecondHighestSalary']].iloc[1:2]
    else :
        return pd.DataFrame({'SecondHighestSalary': [None]}) 
    
    
    # return df[['SecondHighestSalary']].iloc[1:2] is very important as the value  is 2d
    # return df[['SecondHighestSalary']].iloc[1]  ---------------  this become 1d due to change in the iloc value 