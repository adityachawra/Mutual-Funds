import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import urllib3
urllib3.disable_warnings()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('morningstar_scraper.log'),
        logging.StreamHandler()
    ]
)

class MorningstarScraper:
    def __init__(self, max_workers=3):
        self.base_url = "https://www.morningstar.in/gold-rated-mutual-fund.aspx"
        self.drivers = []
        self.funds_data = []
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.progress_queue = Queue()
        
    def setup_driver(self):
        """Initialize the Selenium WebDriver"""
        options = Options()
        options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_window_size(1920, 1080)
        return driver
        
    def random_sleep(self, min_seconds=1, max_seconds=2):
        """Sleep for a random amount of time"""
        time.sleep(random.uniform(min_seconds, max_seconds))
        
    def get_total_pages(self, driver):
        """Get total number of pages"""
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mds-pagination"))
            )
            
            total_results = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblTotalRows").text
            results_per_page = int(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddlShowRowCount").get_attribute("value"))
            
            total_pages = (int(total_results) + results_per_page - 1) // results_per_page
            logging.info(f"Found total pages: {total_pages}")
            return total_pages
        except Exception as e:
            logging.error(f"Error getting total pages: {str(e)}")
            return 1

    def get_fund_links(self, driver):
        """Get all fund links from current page"""
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mds-data-table"))
            )
            
            fund_rows = driver.find_elements(By.CSS_SELECTOR, ".mds-data-table__row")
            logging.info(f"Found {len(fund_rows)} fund rows")
            
            page_funds = []
            for row in fund_rows:
                try:
                    name_element = row.find_element(By.CSS_SELECTOR, ".identity-cell-value")
                    fund_name = name_element.text
                    fund_link = name_element.get_attribute("href")
                    
                    rating_element = row.find_element(By.CSS_SELECTOR, ".analyst-rating-icon-gold")
                    rating = rating_element.get_attribute("title")
                    
                    category_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                    category = category_element.text.replace("Category : ", "").strip()
                    
                    page_funds.append({
                        "name": fund_name,
                        "link": fund_link,
                        "rating": rating,
                        "category": category
                    })
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    continue
                    
            return page_funds
        except Exception as e:
            logging.error(f"Error getting fund links: {str(e)}")
            return []

    def get_fund_isin(self, driver, fund_link, retries=3):
        """Get ISIN for a specific fund with retry logic"""
        for attempt in range(retries):
            try:
                driver.get(fund_link)
                self.random_sleep()
                
                # Wait for ISIN element with specific ID
                try:
                    isin_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_ucQuoteHeader_lblISIN"))
                    )
                    isin = isin_element.text.strip()
                    if isin:
                        return isin
                except:
                    pass
                
                # Fallback to other selectors if the specific ID is not found
                selectors = [
                    "//th[contains(text(), 'ISIN')]/following-sibling::td",
                    "//td[contains(text(), 'ISIN')]/following-sibling::td",
                    "//div[contains(text(), 'ISIN')]/following-sibling::div",
                    "//span[contains(text(), 'ISIN')]/following-sibling::span"
                ]
                
                for selector in selectors:
                    try:
                        element = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                        isin = element.text.strip()
                        if isin:
                            return isin
                    except:
                        continue
                
                if attempt == retries - 1:
                    logging.error(f"Could not find ISIN for {fund_link}")
                    return None
                    
            except Exception as e:
                if attempt == retries - 1:
                    logging.error(f"Error getting ISIN for {fund_link}: {str(e)}")
                    return None
                self.random_sleep()

    def process_fund(self, driver, fund):
        """Process a single fund to get its ISIN"""
        isin = self.get_fund_isin(driver, fund["link"])
        fund["isin"] = isin
        with self.lock:
            self.funds_data.append(fund)
            self.progress_queue.put(1)
        return fund

    def get_page_postback_args(self, driver):
        """Extract all page number postback arguments from the pagination bar, with fallback and debug logging."""
        try:
            # Wait for pagination bar to appear (by class or by id)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'mds-pagination'))
                )
                pagination = driver.find_element(By.CLASS_NAME, 'mds-pagination')
            except Exception:
                # Fallback: try by id (from your screenshot)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_pagerQualitativeRatings'))
                    )
                    pagination = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_pagerQualitativeRatings')
                except Exception:
                    # Log HTML for debugging
                    html = driver.page_source
                    with open('pagination_debug.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                    logging.error('Pagination bar not found. HTML dumped to pagination_debug.html')
                    return []
            page_links = pagination.find_elements(By.TAG_NAME, 'a')
            postbacks = []
            for link in page_links:
                href = link.get_attribute('href')
                text = link.text.strip()
                if href and '__doPostBack' in href and text.isdigit():
                    import re
                    match = re.search(r"__doPostBack\('([^']+)'", href)
                    if match:
                        postbacks.append((text, match.group(1)))
            return postbacks
        except Exception as e:
            logging.error(f"Error extracting page postbacks: {str(e)}")
            return []

    def go_to_next_page(self, driver, prev_first_fund, use_postback=False, postback_arg=None):
        """Go to the next page using postback (first page) or click (subsequent pages)."""
        try:
            if use_postback and postback_arg:
                driver.execute_script(f"__doPostBack('{postback_arg}','')")
            else:
                # Use .r_pager a[title='Next'] for subsequent pages
                next_btn = driver.find_element(By.CSS_SELECTOR, ".r_pager a[title='Next']")
                next_btn.click()
            # Wait for the first fund name to change
            WebDriverWait(driver, 15).until(
                lambda d: self.get_first_fund_name(d) != prev_first_fund
            )
            self.random_sleep(1, 2)
            return True
        except Exception as e:
            logging.error(f"Error going to next page: {str(e)}")
            return False

    def get_first_fund_name(self, driver):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mds-data-table"))
            )
            fund_rows = driver.find_elements(By.CSS_SELECTOR, ".mds-data-table__row")
            if fund_rows:
                name_element = fund_rows[0].find_element(By.CSS_SELECTOR, ".identity-cell-value")
                return name_element.text.strip()
            return None
        except Exception:
            return None

    def wait_for_pager(self, driver, timeout=20):
        """Wait for the pagination bar to be present and visible."""
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div.r_pager"))
        )

    def click_until_page_visible_and_select(self, driver, target_page):
        """Click 'Next' (by text) until the target page number is visible, then click it. Wait for .r_pager. Log HTML if navigation fails. Handle <a> and <span> in .r_pager."""
        for _ in range(100):  # Prevent infinite loop
            try:
                # Wait for pagination bar to appear and be visible
                self.wait_for_pager(driver, timeout=30) # Increased timeout

                pager = driver.find_element(By.CSS_SELECTOR, ".r_pager")
                page_links = pager.find_elements(By.TAG_NAME, "a")
                page_spans = pager.find_elements(By.TAG_NAME, "span")
                # Try to find the page number link (a or span)
                for link in page_links:
                    if link.text.strip() == str(target_page):
                        link.click()
                        self.random_sleep(1, 2)
                        self.wait_for_pager(driver) # Wait again after clicking
                        return True
                for span in page_spans:
                    if span.text.strip() == str(target_page):
                        # Already on this page
                        return True
                # If not found, click the 'Next' button by text
                next_btn = None
                for link in page_links:
                    if link.text.strip().lower() == "next":
                        next_btn = link
                        break
                if not next_btn or 'disabled' in next_btn.get_attribute('class') or not next_btn.is_enabled():
                    # Log HTML for debugging
                    html = pager.get_attribute('outerHTML')
                    with open('pagination_debug.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                    logging.info(f"End of pagination reached or Next button not clickable on page {driver.current_url}")
                    return False
                next_btn.click()
                self.random_sleep(2, 3) # Longer wait after clicking next
                self.wait_for_pager(driver) # Wait for pager to reload after clicking Next
            except Exception as e:
                # Log HTML for debugging
                try:
                    # Try to find the pager again if the initial wait failed
                    pager = driver.find_element(By.CSS_SELECTOR, ".r_pager")
                    html = pager.get_attribute('outerHTML')
                except Exception:
                    # If pager still not found, log the whole page source
                    html = driver.page_source

                with open('pagination_debug.html', 'w', encoding='utf-8') as f:
                    f.write(html)
                logging.error(f"Error navigating to page {target_page}: {str(e)}. HTML dumped to pagination_debug.html")

                # Consider if the error is recoverable or should break the loop
                # For now, break on any navigation exception after trying to log
                return False
        # If loop completes without finding the page or reaching end, something is wrong
        logging.error(f"Failed to reach page {target_page} after multiple attempts.")
        return False

    def is_next_button_available(self, driver):
        """Check if the Next button is visible and enabled."""
        try:
            # Wait for the Next button to be present
            next_btn = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.r_pager a[title='Next']"))
            )
            # Check if the button is visible and enabled
            return next_btn.is_displayed() and next_btn.is_enabled()
        except (NoSuchElementException, TimeoutException):
            return False

    def click_next_button(self, driver, prev_first_fund):
        """Find and click the Next button, then wait for the page to load."""
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "div.r_pager a[title='Next']")
            next_btn.click()
            self.random_sleep(2, 3) # Longer wait after clicking next
            # Wait for the table content to change to confirm navigation
            WebDriverWait(driver, 20).until(
                 lambda d: self.get_first_fund_name(d) != prev_first_fund
             )
            return True
        except Exception as e:
            logging.error(f"Error clicking Next button: {str(e)}")
            return False

    def scrape(self):
        """Main scraping function: paginate using only the 'Next' button clicks."""
        try:
            self.drivers = [self.setup_driver() for _ in range(self.max_workers)]
            main_driver = self.drivers[0]
            logging.info("Starting scraping process...")
            main_driver.get(self.base_url)
            self.random_sleep()

            # We might not get the correct total pages with this method, so just log the initial estimate
            try:
                total_pages = self.get_total_pages(main_driver)
                logging.info(f"Estimated total pages to scrape: {total_pages}")
            except Exception:
                 logging.info("Could not get total page estimate, will scrape until 'Next' button is disabled.")
                 total_pages = 0 # Set to 0 if estimate failed, will scrape until Next button disappears

            page = 1
            # Use total_pages for tqdm if estimated, otherwise use a large number or None
            pbar = tqdm(total=total_pages if total_pages > 0 else None, desc="Scraping pages")

            while True:
                logging.info(f"Processing page {page}")

                # Wait for the table to be present before scraping
                WebDriverWait(main_driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "mds-data-table"))
                )

                page_funds = self.get_fund_links(main_driver)

                if not page_funds:
                    logging.warning(f"No funds found on page {page}. This might be the end or a temporary issue.")
                    # If no funds found on a page, and it's not the first page, assume it's the end
                    if page > 1:
                         break
                    # If it's the first page and no funds, something is fundamentally wrong
                    elif page == 1: # Double check if the first page was empty
                         if not self.funds_data:
                              logging.error("Failed to scrape any funds from the first page. Check selectors.")
                              break
                         pass # Continue if funds were added but this specific call returned empty

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    # Assign drivers to funds in a round-robin fashion
                    for i, fund in enumerate(page_funds):
                        driver = self.drivers[i % self.max_workers]
                        futures.append(executor.submit(self.process_fund, driver, fund))

                    for future in as_completed(futures):
                        try:
                            # Process results or handle exceptions
                            future.result()
                        except Exception as e:
                            logging.error(f"Error processing fund data in thread: {str(e)}")

                self.save_progress()
                pbar.update(1)

                # --- Pagination Logic ---
                # Check if the Next button is available (visible and enabled)
                if self.is_next_button_available(main_driver):
                     logging.info(f"Next button available on page {page}. Attempting to click.")
                     # Get the first fund name BEFORE clicking next to verify page change
                     prev_first_fund = self.get_first_fund_name(main_driver)
                     if self.click_next_button(main_driver, prev_first_fund):
                         page += 1
                         # The loop continues to the next iteration to scrape the new page
                     else:
                         logging.error(f"Failed to click or verify navigation after clicking Next on page {page}.")
                         break # Break loop on navigation failure
                else:
                    logging.info(f"Next button not available on page {page}. End of pagination.")
                    break # End the loop if Next button is not available

            # Ensure the progress bar is closed even if the loop breaks early
            pbar.close()

        except Exception as e:
            logging.error(f"Critical error during scraping process: {str(e)}")
        finally:
            # Ensure all drivers are closed
            for driver in self.drivers:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass

    def save_progress(self):
        """Save current progress to JSON file"""
        try:
            with open('morningstar_funds.json', 'w') as f:
                json.dump(self.funds_data, f, indent=2)
            logging.info(f"Progress saved. Total funds scraped: {len(self.funds_data)}")
        except Exception as e:
            logging.error(f"Error saving progress: {str(e)}")

if __name__ == "__main__":
    scraper = MorningstarScraper(max_workers=3)
    scraper.scrape() 