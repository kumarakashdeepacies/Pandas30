import pandas as pd

def order_scores(scores: pd.DataFrame) -> pd.DataFrame:
    df = scores.sort_values(by='score', ascending=False)
    df['rank'] = df['score'].rank(method='max', ascending=False)
    return df[['score','rank']]



# min of the group 
df['rank'] = df['score'].rank(method='min', ascending=False)



# min but continous ranking no jump's and gaps 
df['rank'] = df['score'].rank(method='max', ascending=False)