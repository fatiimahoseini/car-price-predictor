import pandas as pd
import numpy as np
import os
import re
import jdatetime

def load_and_concat_raw_data(data_raw_path='../data/raw/'):
    
    """Loads and concatenates all raw CSV files."""
    all_files = [os.path.join(data_raw_path, f) for f in os.listdir(data_raw_path) if f.startswith('bama_raw_data_') and f.endswith('.csv')]

    if not all_files:
        print("No raw data CSV files found in the specified directory.")
        return pd.DataFrame()

    df_list = []
    for f in all_files:
        try:
            df_list.append(pd.read_csv(f, encoding='utf-8-sig'))
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    if not df_list:
        print("No dataframes could be loaded.")
        return pd.DataFrame()

    df = pd.concat(df_list, ignore_index=True)
    return df

def normalize_persian_words(text):
    """Normalizes Persian words by adding spaces where commonly removed."""
    if pd.isna(text):
        return text
    text = str(text) 
    # Add your complete replacements dictionary here based on your EDA
    replacements = {
        'نوعسوخت': 'نوع سوخت',
        'دندهای': 'دنده‌ای',
        'اتوماتیک': 'اتوماتیک', 
        'بدونرنگ': 'بدون رنگ',
        'دولکهرنگ': 'دو لکه رنگ',
        'دوررنگ': 'دور رنگ',
        'چندلکهرنگ': 'چند لکه رنگ',
        'کاپوتتعویض': 'کاپوت تعویض',
        'صافکاریبدونرنگ': 'صافکاری بدون رنگ',
        'گلگیرتعویض': 'گلگیر تعویض',
        'یکلکهرنگ': 'یک لکه رنگ',
        'کاملرنگ': 'کامل رنگ',
        'نقرآبی': 'نقره آبی',
        'داخلمشکی': 'داخل مشکی',
        'داخلقهوهای': 'داخل قهوه‌ای',
        'داخلکرم': 'داخل کرم',
        'داخلخاکستری': 'داخل خاکستری',
        'داخلطوسی': 'داخل طوسی',
        'داخلنوکمدادی': 'داخل نوک مدادی',
        'داخلمسی': 'داخل مسی',
        'داخلسفید': 'داخل سفید',
        'پانورامادندهای': 'پانوراما دنده‌ای',
        'پانورامااتوماتیکTU5P': 'پانوراما اتوماتیک TU5P',
        'دندهایTU5': 'دنده ای TU5',
        'GLXدوگانهسوز': 'GLX دوگانه سوز',
        'MCاتوماتیک': 'MC اتوماتیک',
        'GLXبنزینی': 'GLX بنزینی',
        'کارکرده': np.nan, # Handle this as NaN for mileage column
    }
    return replacements.get(text, text) # Return normalized if found, else original


def preprocess_dataframe(df, output_cleaned_dir='data/cleaned/', base_url_identifier=None):
    """
    Applies all necessary preprocessing steps to the raw DataFrame
    and saves the cleaned data to a CSV file.

    Args:
        df (pd.DataFrame): The raw DataFrame to preprocess.
        output_cleaned_dir (str): Directory to save the cleaned data.
        base_url_identifier (str, optional): An identifier from the base URL
                                             to include in the filename. Defaults to None.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """
    if df.empty:
        print("Input DataFrame for preprocessing is empty.")
        return df
    
    # Deduplication
    df.sort_values(by='scrape_date', ascending=False, inplace=True)
    df.drop_duplicates(subset=['ad_url'], keep='first', inplace=True)

    # Clean Price
    df['price'] = df['price'].replace('توافقی', np.nan)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df.dropna(subset=['price'], inplace=True)

    # Clean Mileage
    df['mileage'] = df['mileage'].apply(normalize_persian_words) # Apply normalization for 'کارکرده'
    df['mileage'] = df['mileage'].replace('صفر', '0')
    df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce')
    df.dropna(subset=['mileage'], inplace=True)
    df['mileage'] = df['mileage'].astype(int)

    # Clean Year and Calculate Car Age
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df.dropna(subset=['year'], inplace=True)
    df['year'] = df['year'].astype(int)
    
    current_date_jalali = jdatetime.date.today()
    current_persian_year = current_date_jalali.year
    
    df['car_age'] = current_persian_year - df['year']
    df = df[df['car_age'] >= 0] 

    # Clean Brand
    df['brand'] = df['brand'].str.replace('،', '', regex=False).str.strip()

    # Normalize specific categorical columns
    cols_to_normalize = ['fuel_type', 'gearbox', 'body_condition', 'body_color', 'interior_color', 'trim_version']
    for col in cols_to_normalize:
        if col in df.columns:
            df[col] = df[col].apply(normalize_persian_words)
    
    # Clean Location (optional: split city/area)
    df['location'] = df['location'].str.strip()

    # Drop columns not needed for modeling (e.g., ad_url, scrape_date, full_title)
    columns_to_drop = ['full_title'] 
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True, errors='ignore')

    # --- START: Save cleaned data ---
    os.makedirs(output_cleaned_dir, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    
    # Construct filename based on identifier if provided
    filename_parts = ['bama_cleaned_data']
    if base_url_identifier:
        filename_parts.append(base_url_identifier)
    filename_parts.append(timestamp)
    
    cleaned_filename = os.path.join(output_cleaned_dir, f"{'_'.join(filename_parts)}.csv")
    
    df.to_csv(cleaned_filename, index=False, encoding='utf-8-sig')
    print(f"\nCleaned data saved to: {cleaned_filename}")
    # --- END: Save cleaned data ---

    return df