import pandas as pd
def one_hot_encode_genres(df) -> pd.DataFrame:
    genres = df['genres'].str.get_dummies(sep=',')
    return pd.concat([df, genres], axis=1).drop(columns=['genres'])

def drop_missing_poster(df) -> pd.DataFrame:
    return df.dropna(subset=['poster_url'])