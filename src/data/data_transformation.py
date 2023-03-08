# Libraries to import
import pandas as pd
import numpy as np

def clean_df(input_df, interpolation_method = 'linear', interpolation_limit = 50):
    """
    NOTE: the input_df must have the correct dtypes already set 
    """

    # Separate columns by numeric, categorical, and date types
    num_cols = input_df.select_dtypes(include=[np.number]).columns
    cat_cols = input_df.select_dtypes(include=['object']).columns.difference(num_cols)
    date_cols = input_df.select_dtypes(include=['datetime']).columns
    
    # Fill NaN values using an interpolation method
    for c in num_cols:
        if input_df[c].isnull().sum() > 0:
            input_df[c].interpolate(method=interpolation_method, limit=interpolation_limit, inplace=True)
    
    # Count unique values and null values in categorical columns and impute missing values with one hot encoding
    for c in cat_cols:
        n_unique = input_df[c].nunique()
        n_null = input_df[c].isnull().sum()
        
        print(f"{c}: unique values = {n_unique}, null values = {n_null}")
        
        if n_null > 0:
            input_df = pd.concat([input_df, pd.get_dummies(input_df[c], prefix=col, dummy_na=True)], axis=1)
            input_df.drop(c, axis=1, inplace=True)
    
    # Check that all dates in date type columns are in the format of yyyy-mm-dd hh:mm:ss
    for col in date_cols:
        for date_val in input_df[col]:
            if not pd.isnull(date_val):
                try:
                    pd.to_datetime(date_val, format='%Y-%m-%d %H:%M:%S')
                except ValueError:
                    print(f"{date_val} in column {col} is not in the format of yyyy-mm-dd hh:mm:ss")
    
    return input_df