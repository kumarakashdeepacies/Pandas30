import pandas as pd

def total_time(emp: pd.DataFrame) -> pd.DataFrame:
    df= emp.rename(columns = {
        'event_day' :'day'
    })

    df['total_time']  = df['out_time'] -  df['in_time']

    df = df[['emp_id','day','total_time']]

    df = df.groupby('day')['total_time'].sum().reset_index()

    return df