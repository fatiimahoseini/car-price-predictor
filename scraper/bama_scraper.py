# scraper/bama_scraper.py
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException

from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import re
from urllib.parse import urlparse

from selenium.webdriver.chrome.service import Service as ChromeService


def clean_text(text, for_label=False): # Added a new argument: for_label
    """Cleans and normalizes Persian text.
    If for_label is True, it only strips and converts Persian digits,
    leaving spaces and other characters for label matching.
    """
    if text:
        text = text.strip()
        persian_digits = '۰۱۲۳۴۵۶۷۸۹'
        english_digits = '0123456789'
        translation_table = str.maketrans(persian_digits, english_digits)
        text = text.translate(translation_table)

        if not for_label: # Apply these replacements only for values, not for labels
            text = text.replace('تومان', '').replace(' ', '').replace(',', '').replace('کیلومتر', '').replace('K.M', '')
    return text

def parse_car_details(detail_soup):
    """
    Parses detailed car information from the ad detail page/modal HTML.
    
    Args:
        detail_soup (BeautifulSoup object): Parsed HTML of the car detail page.
        
    Returns:
        dict: A dictionary containing detailed car information.
    """
    car_details = {}

    # 1. Extract Title (Brand, Model)
    title_h1 = detail_soup.find('h1', class_='bama-ad-detail-title__title')
    if title_h1:
        full_title_text = title_h1.get_text(separator=' ', strip=True)
        car_details['full_title'] = full_title_text
        
        parts = full_title_text.split(' ', 1) 
        car_details['brand'] = parts[0] if len(parts) > 0 else None
        car_details['model'] = parts[1] if len(parts) > 1 else None
        
    # 2. Extract Year and Trim from subtitle holder
    subtitle_holder = detail_soup.find('div', class_='bama-ad-detail-title__subtitle-holder')
    if subtitle_holder:
        subtitles = subtitle_holder.find_all('span', class_='bama-ad-detail-title__subtitle')
        if len(subtitles) >= 1:
            year_text = clean_text(subtitles[0].get_text(strip=True))
            if year_text.isdigit():
                car_details['year'] = year_text
            else:
                car_details['year'] = None
                
            if len(subtitles) >= 2:
                car_details['trim_version'] = clean_text(subtitles[1].get_text(strip=True))
            else:
                car_details['trim_version'] = None
        else:
            car_details['year'] = None
            car_details['trim_version'] = None
    else:
        car_details['year'] = None
        car_details['trim_version'] = None

    # 3. Extract Price
    price_span = detail_soup.find('span', class_='bama-ad-detail-price__price-text')
    if price_span:
        price_text = price_span.get_text(strip=True) 
        car_details['price'] = clean_text(price_text) # Use clean_text for price value
    else:
        car_details['price'] = None

    # 4. Extract Location
    address_span = detail_soup.find('span', class_='address-text')
    car_details['location'] = clean_text(address_span.text) if address_span else None

    # 5. Extract other details from 'bama-vehicle-detail-with-icon__detail-holder'
    detail_holders = detail_soup.find_all('div', class_='bama-vehicle-detail-with-icon__detail-holder')
    for holder in detail_holders:
        label_span = holder.find('span') 
        value_tag = holder.find('p', class_='dir-ltr') or holder.find('p') or holder.find('div', class_='bama-vehicle-detail-with-icon__detail-value') or holder.find('span', recursive=False)
        
        if label_span and value_tag:
            label = clean_text(label_span.text, for_label=True) # Use clean_text with for_label=True for labels
            value = clean_text(value_tag.text) # Use clean_text for values
            
            # Now these conditions should match correctly with spaces
            if 'کارکرد' in label:
                car_details['mileage'] = value
            elif 'نوع سوخت' in label:
                car_details['fuel_type'] = value
            elif 'گیربکس' in label:
                car_details['gearbox'] = value
            elif 'وضعیت بدنه' in label:
                car_details['body_condition'] = value
            elif 'رنگ بدنه' in label:
                car_details['body_color'] = value
            elif 'رنگ داخلی' in label:
                car_details['interior_color'] = value
            elif 'بیمه' in label:
                car_details['insurance_status'] = value
            elif 'تست فنی' in label:
                car_details['technical_inspection_status'] = value
            elif 'آپشن' in label: 
                car_details['options'] = value
    
    return car_details

def get_unique_ad_links_from_page_source(page_source):
    """Extracts unique ad links from the given page source."""
    soup = BeautifulSoup(page_source, 'html.parser')
    ad_links = set()
    ad_holders = soup.find_all('div', class_='bama-ad-holder')
    for holder in ad_holders:
        link_tag = holder.find('a', class_='bama-ad listing')
        if link_tag:
            href = link_tag.get('href')
            if href:
                if not href.startswith('http'):
                    href = f"https://bama.ir{href}" 
                ad_links.add(href)
    return ad_links

def scrape_bama_cars_selenium(base_listing_url, target_ad_count=100, max_scrolls=20):
    """
    Scrapes car data from Bama.ir using Selenium to handle infinite scroll.
    
    Args:
        base_listing_url (str): The initial URL of the listing page.
        target_ad_count (int): The desired minimum number of unique ad links to collect.
        max_scrolls (int): Maximum number of scrolls to attempt before stopping.
        
    Returns:
        pd.DataFrame: A DataFrame containing the scraped car data.
    """
    
    all_cars_data = []
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    options.add_argument('accept-language=fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7')

    script_dir = os.path.dirname(os.path.abspath(__file__))
    chromedriver_name = 'chromedriver'
    if os.name == 'nt': 
        chromedriver_name = 'chromedriver.exe'
    chromedriver_path = os.path.join(script_dir, chromedriver_name)

    if not os.path.exists(chromedriver_path):
        print(f"Error: ChromeDriver not found at {chromedriver_path}. Please download it from https://chromedriver.chromium.org/downloads and place it in the 'scraper' directory.")
        return pd.DataFrame()
    
    service = ChromeService(executable_path=chromedriver_path)

    try:
        driver = webdriver.Chrome(service=service, options=options) 
    except WebDriverException as e:
        print(f"Error initializing WebDriver. Ensure ChromeDriver is compatible with your Chrome browser version. Error: {e}")
        return pd.DataFrame()

    print(f"Starting scraping from {base_listing_url} using Selenium...")

    ad_links_to_scrape = set()
    initial_ad_count = 0
    scroll_count = 0

    try:
        driver.get(base_listing_url)
        print(f"Navigated to: {base_listing_url}")

        ad_links_to_scrape.update(get_unique_ad_links_from_page_source(driver.page_source))
        initial_ad_count = len(ad_links_to_scrape)
        print(f"Initially collected {initial_ad_count} ads.")

        while len(ad_links_to_scrape) < target_ad_count and scroll_count < max_scrolls:
            last_height = driver.execute_script("return document.body.scrollHeight")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.body.scrollHeight") > last_height
                )
            except TimeoutException:
                print("No more content loaded after scrolling (scrollHeight did not change). Reached end of page or content.")
                break 

            time.sleep(random.uniform(5, 8)) 

            new_links = get_unique_ad_links_from_page_source(driver.page_source)
            if new_links.issubset(ad_links_to_scrape): 
                print("No truly new ads found after scrolling. Possible end of content or repetitive ads.")
                break
            
            ad_links_to_scrape.update(new_links)
            scroll_count += 1
            print(f"Scrolled {scroll_count} times. Collected {len(ad_links_to_scrape)} unique ads so far (Target: {target_ad_count}).")
            
    except Exception as e:
        print(f"An error occurred during initial page load or scrolling: {e}")
    finally:
        driver.quit()

    all_ad_links_list = list(ad_links_to_scrape)
    print(f"\nCollected a total of {len(all_ad_links_list)} unique ad links for detail scraping.")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
    }

    for i, ad_url in enumerate(all_ad_links_list):
        print(f"Scraping details from ad {i+1}/{len(all_ad_links_list)}: {ad_url}")
        
        try:
            response = requests.get(ad_url, headers=headers)
            response.raise_for_status()
            detail_soup = BeautifulSoup(response.text, 'html.parser')
            
            detail_section = detail_soup.find('div', class_='bama-ad-detail-section')
            if not detail_section: 
                detail_section = detail_soup.find('div', class_='bama-ad-detail-wrapper') 

            if detail_section: 
                car_data = parse_car_details(detail_section)
                car_data['ad_url'] = ad_url 
                car_data['scrape_date'] = pd.Timestamp.now()
                all_cars_data.append(car_data)
            else:
                print(f"Could not find primary detail section (bama-ad-detail-section or bama-ad-detail-wrapper) for {ad_url}.")

            time.sleep(random.uniform(1, 2)) 
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ad detail page {ad_url}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing ad {ad_url}: {e}")

    df = pd.DataFrame(all_cars_data)
    print("Scraping finished.")
    return df

if __name__ == "__main__":
    output_dir = '../data/raw/'
    os.makedirs(output_dir, exist_ok=True) 

    bama_base_url_for_selenium = 'https://bama.ir/car?mileage=1' 

    parsed_url = urlparse(bama_base_url_for_selenium)
    path_segments = [s for s in parsed_url.path.split('/') if s] 
    
    filename_identifier = 'all_cars' 
    if len(path_segments) >= 2 and path_segments[0] == 'car':
        filename_identifier = path_segments[-1] 
    
    target_ads = 500 
    max_scrolls_limit = 70 

    scraped_df = scrape_bama_cars_selenium(bama_base_url_for_selenium, 
                                           target_ad_count=target_ads, 
                                           max_scrolls=max_scrolls_limit) 
    
    if not scraped_df.empty:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_filename = os.path.join(output_dir, f'bama_raw_data_{filename_identifier}_{timestamp}.csv')
        scraped_df.to_csv(output_filename, index=False, encoding='utf-8-sig') 
        print(f"Scraped data saved to: {output_filename}")
    else:
        print("No data was scraped.")