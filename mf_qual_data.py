import pandas as pd
import time
import json
from playwright.sync_api import sync_playwright

def scrape_morningstar_funds():
    # URL of the gold-rated mutual funds page
    url = "https://www.morningstar.in/gold-rated-mutual-fund.aspx"
    
    # List to store all fund data
    all_funds_data = []
    
    with sync_playwright() as p:
        try:
            print("Launching browser...")
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            print(f"Attempting to fetch data from: {url}")
            page.goto(url)
            
            # Wait for the table to be present
            print("Waiting for table to load...")
            page.wait_for_selector("table")
            
            # Get all rows from the table
            rows = page.query_selector_all("table tr")[1:]  # Skip header row
            print(f"Found {len(rows)} rows in the table")
            
            for row in rows:
                try:
                    # Extract fund name and link
                    fund_cell = row.query_selector(".fund-name")
                    if not fund_cell:
                        print("Fund cell not found in row")
                        continue
                        
                    fund_link = fund_cell.query_selector("a")
                    if not fund_link:
                        print("Fund link not found in cell")
                        continue
                    
                    fund_name = fund_link.inner_text().strip()
                    fund_url = fund_link.get_attribute("href")
                    
                    print(f"Processing fund: {fund_name}")
                    
                    # Get detailed fund information
                    fund_details = get_fund_details(fund_url, page)
                    
                    # Extract other information from the row
                    cells = row.query_selector_all("td")
                    category = cells[1].inner_text().strip() if len(cells) > 1 else "N/A"
                    medalist_rating = cells[2].inner_text().strip() if len(cells) > 2 else "N/A"
                    star_rating = cells[5].inner_text().strip() if len(cells) > 5 else "N/A"
                    
                    # Combine all information
                    fund_data = {
                        'Fund Name': fund_name,
                        'Category': category,
                        'Medalist Rating': medalist_rating,
                        'Star Rating': star_rating,
                        'ISIN': fund_details.get('isin', 'N/A'),
                        'Fund URL': fund_url
                    }
                    
                    all_funds_data.append(fund_data)
                    print(f"Successfully added fund: {fund_name}")
                    
                    # Add a small delay to avoid overwhelming the server
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"Error processing row: {str(e)}")
                    continue
            
            browser.close()
            
            # Create DataFrame
            df = pd.DataFrame(all_funds_data)
            return df
            
        except Exception as e:
            print(f"Error scraping data: {str(e)}")
            return None

def get_fund_details(url, page):
    """Get detailed information for a specific fund"""
    try:
        page.goto(url)
        
        # Wait for the page to load and find ISIN
        isin = "N/A"
        try:
            isin_element = page.wait_for_selector("//div[contains(text(), 'ISIN')]/following-sibling::div", timeout=10000)
            if isin_element:
                isin = isin_element.inner_text().strip()
        except:
            print("Could not find ISIN for fund")
        
        return {'isin': isin}
        
    except Exception as e:
        print(f"Error getting fund details: {str(e)}")
        return {'isin': 'N/A'}

# Run the scraper
if __name__ == "__main__":
    print("Starting to scrape Morningstar mutual fund data...")
    df = scrape_morningstar_funds()
    
    if df is not None and not df.empty:
        # Save to JSON
        output_file = "morningstar_funds_data.json"
        df.to_json(output_file, orient='records', indent=4)
        print(f"Data successfully saved to {output_file}")
        
        # Display first few rows
        print("\nFirst few rows of the data:")
        print(df.head())
    else:
        print("Failed to scrape data or no data was found")
