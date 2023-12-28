import pandas as pd

def nth_highest_salary(employee: pd.DataFrame, N: int) -> pd.DataFrame:
    df = employee.sort_values(by='salary', ascending=False)
    s = 'getNthHighestSalary({})'.format(N)
    df.rename(columns={'salary': s}, inplace=True)
    if len(df) < N:
        return pd.DataFrame({s: [None]})
    else:
        return df[[s]].iloc[N-1:N]


# the code is optimize if the entries are less then it will give null values 





# new question what if the value of N become's negative  
# n == -1 ?? then 
