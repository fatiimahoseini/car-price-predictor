import pandas as pd
import numpy as np
import os
import re
import jdatetime
from urllib.parse import urlparse 

def clean_numeric_string(text):
    """
    Cleans numeric strings (price, mileage) by removing non-digit chars (except dot for floats)
    and converting Persian digits.
    """
    if pd.isna(text) or text is None:
        return text
    text = str(text).strip()
    
    # Replace Persian digits with English digits
    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
    english_digits = '0123456789'
    translation_table = str.maketrans(persian_digits, english_digits)
    text = text.translate(translation_table)

    # Remove currency, mileage units, and spaces/commas from numbers
    # Ensure to only remove 'km' related strings, not actual numbers
    text = text.replace('تومان', '').replace(' ', '').replace(',', '')
    # Specific for mileage: remove 'km', 'KM', 'کیلومتر'
    text = text.replace('km', '').replace('KM', '').replace('کیلومتر', '')
    
    return text

def normalize_persian_words(text):
    """
    Normalizes Persian words (labels/categories) by adding spaces where commonly removed
    and handles specific non-numeric/non-standard categorical values.
    """
    if pd.isna(text) or text is None:
        return text
    text = str(text).strip()
    
    # Specific replacements for categorical values
    replacements = {
        # Mileage specific replacements
        'صفر': '0', # Only 'صفر' becomes '0' here, others will go through clean_numeric_string or become NaN
        # 'کارکرده': np.nan, # 'کارکرده' directly becomes NaN

        # Categorical labels that had spaces removed by early clean_text (now fixed in scraper)
        # But we keep these mappings in case old raw data or future inconsistencies.
        'نوعسوخت': 'نوع سوخت',
        'دندهای': 'دنده ای',
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
        'پانورامادندهای': 'پانوراما دنده ای',
        'پانورامااتوماتیکTU5P': 'پانوراما اتوماتیک TU5P',
        'دندهایTU5': 'دنده ای TU5',
        'GLXدوگانهسوز': 'GLX دوگانه سوز',
        'MCاتوماتیک': 'MC اتوماتیک',
        'GLXبنزینی': 'GLX بنزینی',
        'اتوماتیکاسپرتاکسلنت': 'اتوماتیک اسپرت اکسلنت',
        'پلاسEF7دوگانهسوز' : 'پلاس EF7 دوگانه سوز',
        'تیپ2هفتنفره':'تیپ 2 هفت نفره',
        'تیپ1پنجنفره':'تیپ 1 پنج نفره',
        'اتوماتیکتوربوآپشنال':'اتوماتیک توربو آپشنال',
        'دندهایبنزینی':'دنده ای بنزینی',
        'دوگانهسوز':'دوگانه سوز',
        'اتوماتیکتوربوساده':'اتوماتیک توربو ساده',
        '5دندهساده':'5 دنده ساده',
        '6دندهتوربو':'6 دنده توربو',
        'دنده ای اسپرت اکسلنت':'دنده ای اسپرت اکسلنت',
        'اسپرتاکسلنت':'اسپرت اکسلنت'
    }
    
    return replacements.get(text, text)

def load_and_concat_raw_data(data_raw_path='data/raw/'): # Adjusted path for module import context
    """Loads and concatenates all raw CSV files."""
    all_files = [os.path.join(data_raw_path, f) for f in os.listdir(data_raw_path) if f.startswith('bama_raw_data_') and f.endswith('.csv')]
    
    if not all_files:
        print("No raw data CSV files found in the specified directory.")
        return pd.DataFrame()

    df_list = []
    for f in all_files:
        try:
            df_list.append(pd.read_csv(f, encoding='utf-8-sig', low_memory=False)) 
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
    
    if not df_list:
        print("No dataframes could be loaded.")
        return pd.DataFrame()

    df = pd.concat(df_list, ignore_index=True)
    return df

def preprocess_dataframe(df, output_cleaned_dir='data/cleaned/'):
    """
    Applies all necessary preprocessing steps to the raw DataFrame
    and saves the cleaned data to a CSV file.

    Args:
        df (pd.DataFrame): The raw DataFrame to preprocess.
        output_cleaned_dir (str): Directory to save the cleaned data.
                                  (e.g., 'data/cleaned/' which is relative to project root)

    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """
    if df.empty:
        print("Input DataFrame for preprocessing is empty.")
        return df
    
    print(f"Shape before preprocessing: {df.shape}")

    # Deduplication
    df.sort_values(by='scrape_date', ascending=False, inplace=True)
    df.drop_duplicates(subset=['ad_url'], keep='first', inplace=True)
    print(f"Shape after deduplication: {df.shape}")

    # Clean Price
    df['price'] = df['price'].apply(clean_numeric_string) # Apply cleaning for numeric string
    df['price'] = df['price'].replace('توافقی', np.nan) # Handle 'توافقی' explicitly
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df.dropna(subset=['price'], inplace=True)
    print(f"Shape after price cleaning: {df.shape}")

    # Clean Mileage
    # Apply normalize_persian_words first to handle 'صفر' 
    df['mileage'] = df['mileage'].apply(normalize_persian_words)
    # Then apply clean_numeric_string to remove 'km' and convert digits for actual numbers
    df['mileage'] = df['mileage'].apply(clean_numeric_string)
    df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce')
    df.dropna(subset=['mileage'], inplace=True)
    df['mileage'] = df['mileage'].astype(int)
    print(f"Shape after mileage cleaning: {df.shape}")

    # Clean Year and Calculate Car Age
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df.dropna(subset=['year'], inplace=True)
    df['year'] = df['year'].astype(int)
    
    current_date_jalali = jdatetime.date.today()
    current_persian_year = current_date_jalali.year
    
    df['car_age'] = current_persian_year - df['year']
    df = df[df['car_age'] >= 0] 
    print(f"Shape after year/car_age cleaning: {df.shape}")

    # Clean Brand
    df['brand'] = df['brand'].str.replace('،', '', regex=False).str.strip()

    # Normalize specific categorical columns
    cols_to_normalize = ['fuel_type', 'gearbox', 'body_condition', 'body_color', 'interior_color', 'trim_version']
    for col in cols_to_normalize:
        if col in df.columns:
            df[col] = df[col].apply(normalize_persian_words) 
        
    # Clean Location (optional: split city/area)
    df['location'] = df['location'].str.strip()

    # Drop columns not needed for modeling
    columns_to_drop = ['full_title'] 
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True, errors='ignore')

    # Save cleaned data
    # Construct the full absolute path for saving to data/cleaned/ from project root
    # This is critical for saving from utils/preprocess.py when called from notebooks/
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Path to utils folder
    project_root = os.path.dirname(script_dir) # Path to car-price-predictor folder
    
    full_output_dir = os.path.join(project_root, output_cleaned_dir) # Combine project root with relative data/cleaned path
    os.makedirs(full_output_dir, exist_ok=True)
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    cleaned_filename = os.path.join(full_output_dir, f"bama_cleaned_data_{timestamp}.csv") 
    df.to_csv(cleaned_filename, index=False, encoding='utf-8-sig')
    print(f"\nCleaned data saved to: {cleaned_filename}")

    return df