def prepare_store_data():
    import acquire as a
    import pandas as pd 
    import os
    df = a.get_store_data()
    df ['sale_date']= pd.to_datetime(df['sale_date'])
    df = df.set_index('sale_date').sort_index()
    df['month'] = df.index.month_name()
    df['day'] = df.index.day_name()
    df['total_price'] = df['item_price'] * df['sale_amount']
    return df
