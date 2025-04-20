import random
import json
import time
import traceback
import unicodedata
from seleniumbase import SB
import threading
import copy
from datetime import datetime
import os
from fake_useragent import UserAgent
import filelock

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Directory of this script
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "project_descriptions.json") # Output file for scraped data
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, 'scraping_checkpoint.json')  # Checkpoint file for tracking progress
ERROR_LOG_FILE = os.path.join(SCRIPT_DIR, 'scraping_errors.json')   # Error log file for tracking errors
error_log_lock = threading.Lock()  # Lock for thread-safe access to error log
MAX_REQUESTS_PER_MINUTE = 20    # Maximum requests allowed per minute
rate_limiter_lock = threading.Lock()    # Lock for thread-safe access to rate limiter

def get_random_user_agent():
    """
    Generates a random user agent string.

    Uses the fake_useragent library if available, otherwise falls back to a
    predefined static list of common user agents.

    Returns:
        str: A randomly selected user agent string.
    """
    try:
        return UserAgent().random
    except Exception as e:
        print(f"Warning: Error generating random user agent with fake_useragent: {e}. Falling back to static list.")
        return random.choice(agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.2365.92",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/123.0"
        ])

class RateLimiter:
    """A simple rate limiter to ensure we don't exceed MAX_REQUESTS_PER_MINUTE"""
    def __init__(self, max_requests, time_window=60):
        """
        Initializes the RateLimiter.

        Args:
            max_requests (int): The maximum number of requests allowed within the time window.
            time_window (int, optional): The time window in seconds. Defaults to 60.
        """
        self.max_requests = max_requests  # Maximum requests in time window
        self.time_window = time_window    # Time window in seconds
        self.request_timestamps = []      # List of request timestamps
        self.lock = threading.Lock()      # Lock for thread safety

    def wait_if_needed(self):
        """
        Checks if the rate limit has been reached and waits if necessary.

        This method is thread-safe. It removes timestamps older than the
        time window, checks if the current number of requests exceeds the limit,
        and sleeps for the required duration if the limit is reached. It then
        adds the current timestamp to the list.

        Returns:
            int: The current number of requests within the time window after waiting (if needed)
                 and adding the new request timestamp.
        """
        with self.lock:
            current_time = time.time()

            cutoff_time = current_time - self.time_window
            self.request_timestamps = [ts for ts in self.request_timestamps if ts > cutoff_time]

            if len(self.request_timestamps) >= self.max_requests:
                oldest_timestamp = min(self.request_timestamps)
                sleep_time = oldest_timestamp + self.time_window - current_time
                
                if sleep_time > 0:
                    print(f"Rate limit reached. Waiting {sleep_time:.2f}s before next request.")
                    time.sleep(sleep_time)
                    current_time = time.time()
                    cutoff_time = current_time - self.time_window
                    self.request_timestamps = [ts for ts in self.request_timestamps if ts > cutoff_time]

            self.request_timestamps.append(current_time)

            return len(self.request_timestamps)

rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)

def load_error_log():
    """
    Loads the error log from the ERROR_LOG_FILE.

    Reads the JSON file containing previously logged errors. If the file
    doesn't exist, it initializes an empty error log structure. This function
    is thread-safe using `error_log_lock`.

    Returns:
        dict: A dictionary containing the error log data, typically with
              'error_urls' and 'error_entries' keys.
    """
    with error_log_lock:
        try:
            with open(ERROR_LOG_FILE, 'r', encoding='utf-8') as f:
                error_log = json.load(f)
        except FileNotFoundError:
            error_log = {
                'error_urls': [],
                'error_entries': []
            }
        return error_log

def save_error_entry(entry, url, error_message):
    """
    Saves a failed entry and its associated error to the error log file.

    Loads the current error log, appends the new error entry (if the URL
    is not already logged), and saves the updated log back to the JSON file.
    This function is thread-safe using `error_log_lock`.

    Args:
        entry (dict): The original data entry that caused the error.
        url (str): The URL that failed to process.
        error_message (str): A description of the error encountered.
    """
    with error_log_lock:
        try:
            with open(ERROR_LOG_FILE, 'r', encoding='utf-8') as f:
                error_log = json.load(f)
        except FileNotFoundError:
            error_log = {
                'error_urls': [],
                'error_entries': []
            }

        if url not in error_log['error_urls']:
            error_log['error_urls'].append(url)

            error_entry = {
                'entry': entry,
                'url': url,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
            error_log['error_entries'].append(error_entry)

            with open(ERROR_LOG_FILE, 'w', encoding='utf-8') as f:
                json.dump(error_log, f, indent=2, ensure_ascii=False)
                
            print(f"Entry logged to error file: {url}")

def process_entry_worker(entry, url, checkpoint, results, worker_id=None):
    """
    Processes a single project entry: scrapes data, updates results, and manages checkpoint.

    This function orchestrates the scraping of a specific project URL using
    `get_description_content`. If scraping is successful, it extracts metadata,
    formats the result, updates the shared results list, and updates the
    checkpoint file to mark the URL as processed. If scraping fails, it logs
    the error using `save_error_entry`. Handles data extraction errors gracefully.

    Args:
        entry (dict): The dictionary containing metadata for the project entry.
                      Expected to have a 'data' key and potentially 'local_proxy'.
        url (str): The URL of the Kickstarter project page to scrape.
        checkpoint (dict): The current checkpoint data (loaded externally).
        results (list): The list of successfully scraped results (loaded externally).
        worker_id (str | int | None, optional): Identifier for the worker thread/process.
                                                 Defaults to None.

    Returns:
        dict | None: The result dictionary from `get_description_content`
                     (containing status, scraped data, or error message),
                     or None if an unexpected error occurs during checkpoint handling.
    """
    user_agent = get_random_user_agent()
    entry_with_agent = copy.deepcopy(entry)
    entry_with_agent['user_agent'] = user_agent
    entry_id = entry['data']['id']
    start_time = time.time()
    result = get_description_content(url, entry_with_agent, checkpoint, results)
    processing_time = time.time() - start_time

    if result and result.get('status') == 'success':
        worker_str = f"Worker {worker_id}: " if worker_id is not None else ""
        
        try:
            latest_checkpoint, latest_results = load_checkpoint()

            if url not in latest_checkpoint['processed_urls']:
                print(f"{worker_str}Adding {url} to checkpoint")
                
                latest_checkpoint['processed_urls'].append(url)
                latest_checkpoint['processed_count'] = len(latest_checkpoint['processed_urls'])
                latest_checkpoint['total_processing_time'] += processing_time

                try:
                    try:
                        raw_goal = float(entry['data']['goal'])
                        usd_exchange_rate = float(entry['data']['usd_exchange_rate'])
                        goal = max(1.0, raw_goal * usd_exchange_rate)
                        
                        pledged = float(entry['data']['converted_pledged_amount'])

                        raised = 0.0 if pledged == 0 else round((pledged / goal) * 100, 2)

                        created_at = int(entry['data']['created_at'])
                        deadline = int(entry['data']['deadline'])

                        campaign_duration = 0
                        if created_at and deadline and created_at < deadline:
                            campaign_duration = (deadline - created_at) // 86400 

                        backer_count = int(entry['data']['backers_count'])
                        category = entry['data']['category']['parent_name']
                        subcategory = entry['data']['category']['name']
                        country = entry['data']['location']['expanded_country']
                        country_code = entry['data']['location']['country']
                        state = entry['data']['state']
                        creator_id = entry['data']['creator']['id']
                        staff_pick = entry['data']['staff_pick']
                    except KeyError as e:
                        print(f"{worker_str}Missing key in data: {str(e)}")
                        if 'raw_goal' not in locals(): raw_goal = 0.0
                        if 'goal' not in locals(): goal = 0.0
                        if 'pledged' not in locals(): pledged = 0.0
                        if 'raw_pledged' not in locals(): raw_pledged = 0.0
                        if 'raised' not in locals(): raised = 0.0
                        if 'created_at' not in locals(): created_at = 0
                        if 'deadline' not in locals(): deadline = 0
                        if 'campaign_duration' not in locals(): campaign_duration = 0
                        if 'backer_count' not in locals(): backer_count = 0
                        if 'category' not in locals(): category = ''
                        if 'subcategory' not in locals(): subcategory = ''
                        if 'country' not in locals(): country = ''
                        if 'country_code' not in locals(): country_code = ''
                        if 'state' not in locals(): state = ''
                        if 'creator_id' not in locals(): creator_id = ''
                        if 'staff_pick' not in locals(): staff_pick = False
                except Exception as e:
                    print(f"{worker_str}Warning: Error processing metadata: {str(e)}")
                    raw_goal, raw_pledged, raw_raised = 0.0, 0.0, 0.0
                    created_at, deadline = 0, 0
                    campaign_duration = 0
                    goal, pledged, raised = 0.0, 0.0, 0.0
                    backer_count = 0
                    category, subcategory, country, country_code, state = '', '', '', '', ''
                    creator_id = ''
                    staff_pick = False

                new_result = {
                    'id': entry_id,  
                    'name': entry['data']['name'],
                    'url': url,
                    'description': result.get('description', ''),
                    'risk': result.get('risk', ''),
                    'image_urls': result.get('image_urls', []),
                    'video_urls': result.get('video_urls', []),
                    'image_count': result.get('image_count', 0),
                    'video_count': result.get('video_count', 0),
                    'goal': goal,
                    'pledged_amount': pledged,
                    'percent_raised': raised,
                    'campaign_duration': campaign_duration,
                    'backer_count': backer_count,
                    'category': category,
                    'subcategory': subcategory,
                    'country': country,
                    'state': state,
                    'creator_id': creator_id,
                    'staff_pick': staff_pick
                }

                existing_idx = None
                for i, item in enumerate(latest_results):
                    if item.get('url') == url:
                        existing_idx = i
                        break
                
                if existing_idx is not None:
                    latest_results[existing_idx] = new_result
                else:
                    latest_results.append(new_result)
                
                success = save_checkpoint_with_verification(latest_checkpoint, latest_results, worker_id)
                if success:
                    print(f"{worker_str}Successfully saved data for {url}")
                else:
                    print(f"{worker_str}Warning: Possible issue with saving data for {url}")
            
        except Exception as e:
            print(f"{worker_str}Error updating checkpoint: {str(e)}")
            traceback.print_exc()  
            
    elif result and result.get('status') == 'error':
        save_error_entry(entry, url, result.get('error', 'Unknown error'))
    
    return result

FILE_LOCK_TIMEOUT = 60 

def load_checkpoint():
    """
    Loads the checkpoint data and existing results using file locking.

    Reads the checkpoint status (processed URLs, counts, time) from
    `CHECKPOINT_FILE` and the accumulated results from `OUTPUT_FILE`.
    Uses `filelock` to prevent race conditions when accessed by multiple
    processes or threads. If files don't exist, initializes with empty data.

    Returns:
        tuple[dict, list]: A tuple containing:
            - checkpoint (dict): The loaded checkpoint data.
            - results (list): The loaded results data.
    """
    checkpoint_lock = filelock.FileLock(f"{CHECKPOINT_FILE}.lock", timeout=FILE_LOCK_TIMEOUT)
    output_lock = filelock.FileLock(f"{OUTPUT_FILE}.lock", timeout=FILE_LOCK_TIMEOUT)
    
    with checkpoint_lock:
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
        except FileNotFoundError:
            checkpoint = {
                'processed_urls': [],
                'processed_count': 0,
                'total_processing_time': 0
            }

    with output_lock:
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                results = json.load(f)
        except FileNotFoundError:
            results = []
    
    return checkpoint, results

def save_checkpoint(checkpoint, results):
    """
    Saves the checkpoint data and results to their respective files using file locking.

    Writes the provided checkpoint dictionary to `CHECKPOINT_FILE` and the
    results list to `OUTPUT_FILE` in JSON format. Uses `filelock` to ensure
    atomic writes and prevent data corruption from concurrent access.

    Args:
        checkpoint (dict): The checkpoint data to save.
        results (list): The list of results to save.
    """
    checkpoint_lock = filelock.FileLock(f"{CHECKPOINT_FILE}.lock", timeout=FILE_LOCK_TIMEOUT)
    output_lock = filelock.FileLock(f"{OUTPUT_FILE}.lock", timeout=FILE_LOCK_TIMEOUT)

    with checkpoint_lock:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2)

    with output_lock:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

def save_checkpoint_with_verification(checkpoint, results, worker_id=None):
    """
    Saves checkpoint and results, then verifies the save operation.

    Attempts to save the data using `save_checkpoint`. After saving, it reloads
    the checkpoint file to verify that all processed URLs were correctly written.
    If verification fails, it attempts to save again using an extended timeout.
    Includes logging for success, failure, and retry attempts.

    Args:
        checkpoint (dict): The checkpoint data to save.
        results (list): The list of results to save.
        worker_id (str | int | None, optional): Identifier for the worker thread/process.
                                                 Defaults to None.

    Returns:
        bool: True if the checkpoint was saved and verified successfully (or saved
              successfully on retry), False otherwise.
    """
    worker_str = f"Worker {worker_id}: " if worker_id is not None else ""
    
    try:
        save_checkpoint(checkpoint, results)
        verify_checkpoint, _ = load_checkpoint()
        missing_urls = set(checkpoint['processed_urls']) - set(verify_checkpoint['processed_urls'])
        
        if missing_urls:
            print(f"{worker_str}WARNING - {len(missing_urls)} URLs not found in checkpoint after saving! Retrying...")
            save_checkpoint_with_extended_timeout(checkpoint, results)
            return False
        else:
            print(f"{worker_str}Successfully verified checkpoint data")
            return True
            
    except Exception as e:
        print(f"{worker_str}Error during checkpoint saving: {str(e)}")
        try:
            save_checkpoint_with_extended_timeout(checkpoint, results)
            print(f"{worker_str}Used extended timeout for checkpoint saving")
            return True
        except Exception as e2:
            print(f"{worker_str}CRITICAL: Checkpoint saving failed even with extended timeout: {str(e2)}")
            return False

def save_checkpoint_with_extended_timeout(checkpoint, results, timeout=120):
    """
    Saves checkpoint and results using an extended file lock timeout.

    This function is typically called as a fallback mechanism when the standard
    `save_checkpoint` might be failing due to lock contention or delays. It uses
    a longer timeout for acquiring the file locks.

    Args:
        checkpoint (dict): The checkpoint data to save.
        results (list): The list of results to save.
        timeout (int, optional): The file lock timeout in seconds. Defaults to 120.
    """
    checkpoint_lock = filelock.FileLock(f"{CHECKPOINT_FILE}.lock", timeout=timeout)
    output_lock = filelock.FileLock(f"{OUTPUT_FILE}.lock", timeout=timeout)
    
    with checkpoint_lock:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2)

    with output_lock:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

def get_description_content(url, entry, checkpoint, results):
    """
    Scrapes the description, risks, images, and videos from a Kickstarter project URL.

    Uses SeleniumBase with Undetected Chromedriver (UC) and CDP (Chrome DevTools Protocol)
    commands for robust scraping. Handles proxies, user agents, rate limiting,
    retries, captchas, and dynamic content loading. Extracts relevant text and media URLs.

    Args:
        url (str): The URL of the Kickstarter project page to scrape.
        entry (dict): The dictionary containing metadata for the project, including
                      'user_agent' and 'local_proxy'.
        checkpoint (dict): The current checkpoint data (used to skip already processed URLs).
        results (list): The list of successfully scraped results (not directly modified here,
                        passed for context but primarily used in `process_entry_worker`).

    Returns:
        dict: A dictionary containing:
            - 'status' (str): 'success', 'skipped', or 'error'.
            - 'description' (str, optional): Scraped project description text (if successful).
            - 'risk' (str, optional): Scraped project risks text (if successful).
            - 'image_urls' (list[str], optional): List of scraped image URLs (if successful).
            - 'video_urls' (list[str], optional): List of scraped video URLs (if successful).
            - 'image_count' (int, optional): Count of unique image URLs (if successful).
            - 'video_count' (int, optional): Count of unique video URLs (if successful).
            - 'url' (str): The processed URL.
            - 'error' (str, optional): Error message (if status is 'error').
    """
    
    if url in checkpoint['processed_urls']:
        print(f"Skipping already processed URL: {url}")
        return {'status': 'skipped', 'url': url}
    
    max_retries = 3
    retry_count = 0
    last_error = None
    worker_id = entry.get('worker_id', 'unknown')  

    agent = entry.get('user_agent', get_random_user_agent())
    print(f"Worker {worker_id}: Using user agent: {agent}")

    with rate_limiter_lock:
        current_requests = rate_limiter.wait_if_needed()
        print(f"Worker {worker_id}: Current request rate: {current_requests}/{MAX_REQUESTS_PER_MINUTE} per minute")

    initial_delay = random.uniform(0.5, 2.0)
    print(f"Worker {worker_id}: Adding initial delay of {initial_delay:.2f}s before scraping {url}")
    time.sleep(initial_delay)

    while retry_count < max_retries:
        sb = None  
        try:
            if 'local_proxy' not in entry:
                raise Exception("No proxy provided. Proxy is required for scraping.")

            proxy_server = entry['local_proxy']
            print(f"Worker {worker_id}: Using proxy: {proxy_server}")

            chrome_args = [
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-extensions",
                "--disable-popup-blocking", "--disable-plugins", "--disable-remote-fonts",
                "--disable-background-timer-throttling", "--disable-hang-monitor", "--disable-sync",
                "--disable-translate", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled",
                "--ignore-certificate-errors", "--ignore-ssl-errors", "--disable-application-cache",
                "--disk-cache-size=0", "--media-cache-size=0", "--aggressive-cache-discard"
            ]

            with SB(
                uc=True,
                agent=agent,
                headless2=True,
                proxy=proxy_server,
                chromium_arg=" ".join(chrome_args)
            ) as sb:
                print(f"Worker {worker_id}: Created new session for {url}")

                navigation_successful = False
                try:
                    print(f"Worker {worker_id}: Navigating to {url} using activate_cdp_mode")
                    sb.activate_cdp_mode(url)
                    sb.sleep(3)

                    if sb.cdp.is_text_visible("err_empty_response") or "ERR_EMPTY_RESPONSE" in sb.get_page_source().upper():
                        print(f"Worker {worker_id}: ERR_EMPTY_RESPONSE detected. Trying refresh...")
                        sb.refresh()
                        sb.sleep(5)
                        if sb.cdp.is_text_visible("err_empty_response") or "ERR_EMPTY_RESPONSE" in sb.get_page_source().upper():
                             raise Exception("ERR_EMPTY_RESPONSE persisted after refresh")
                        else:
                             print(f"Worker {worker_id}: Refresh successful.")
                             navigation_successful = True
                    else:
                        navigation_successful = True

                except StopIteration as si:
                     print(f"Worker {worker_id}: StopIteration during CDP activation: {si}. Trying sb.open()")
                     sb.open(url)
                     sb.sleep(3)
                     navigation_successful = True 
                     if sb.cdp.is_text_visible("err_empty_response") or "ERR_EMPTY_RESPONSE" in sb.get_page_source().upper():
                         raise Exception("ERR_EMPTY_RESPONSE detected after sb.open()")
                except Exception as nav_err:
                     print(f"Worker {worker_id}: Navigation failed: {nav_err}")
                     raise 

                if not navigation_successful:
                     raise Exception("Page navigation ultimately failed.")

                try:
                    print(f"Worker {worker_id}: Checking for captchas...")
                    sb.uc_gui_click_captcha()
                    sb.sleep(1) 
                except Exception:
                    pass

                try:
                    if sb.execute_script("return document.querySelectorAll('iframe[src*=\"captcha\"]').length > 0"):
                        print(f"Worker {worker_id}: Captcha iframe detected, waiting...")
                        sb.sleep(8) 
                except Exception as captcha_js_err:
                    print(f"Worker {worker_id}: Error checking captcha iframe via JS: {captcha_js_err}")

                sb.sleep(random.uniform(2, 4)) 

                print(f"Worker {worker_id}: Verifying page content...")
                content_selector = ".rte__content"
                risks_selector = ".js-risks"
                error_selector = ".loading-error" 

                if sb.cdp.is_element_present(error_selector):
                    raise Exception("Page loading error element found.")

                if not (sb.cdp.is_element_present(content_selector) or sb.cdp.is_element_present(risks_selector)):
                    print(f"Worker {worker_id}: Expected content selectors ('{content_selector}', '{risks_selector}') not found. Waiting and retrying check...")
                    sb.sleep(5)
                    if not (sb.cdp.is_element_present(content_selector) or sb.cdp.is_element_present(risks_selector)):
                       print(f"Worker {worker_id}: Content still not found after wait. Proceeding, but extraction might fail.")
                    else:
                       print(f"Worker {worker_id}: Content appeared after waiting.")
                else:
                    print(f"Worker {worker_id}: Page content verified.")

                description_texts = []
                try:
                    desc_elements = sb.cdp.find_all(f"{content_selector} p, {content_selector} h1, {content_selector} h2, {content_selector} h3, {content_selector} h4, {content_selector} h5, {content_selector} h6, {content_selector} span, {content_selector} > div, {content_selector} i", timeout=8)
                    if desc_elements:
                        for element in desc_elements:
                            text = element.get('text', '') if isinstance(element, dict) else getattr(element, 'text', '')
                            if text:
                                description_texts.append(unicodedata.normalize('NFKC', text.strip()))
                except Exception as e:
                    print(f"Worker {worker_id}: Warning: Error extracting description elements: {str(e)}")

                risk_texts = []
                try:
                    if sb.cdp.is_element_present(risks_selector):
                        has_risk_text = sb.execute_script(f"return document.querySelector('{risks_selector}').innerText.trim().length > 0;")
                        if has_risk_text:
                             risk_elements = sb.cdp.find_all(f"{risks_selector} p", timeout=8)
                             if risk_elements:
                                 for element in risk_elements:
                                     text = element.get('text', '') if isinstance(element, dict) else getattr(element, 'text', '')
                                     if text:
                                        risk_texts.append(unicodedata.normalize('NFKC', text.strip()))
                        else:
                             print(f"Worker {worker_id}: Risk section '{risks_selector}' found but appears empty.")
                    else:
                        print(f"Worker {worker_id}: Risk section '{risks_selector}' not found.")
                except Exception as e:
                    print(f"Worker {worker_id}: Warning: Error extracting risk elements: {str(e)}")

                image_urls = []
                try:
                    if sb.cdp.is_element_present(f"{content_selector} img"):
                        img_elements = sb.cdp.find_all(f"{content_selector} img", timeout=5)
                        if img_elements:
                            for img in img_elements:
                                src = img.get('attributes', {}).get('src') or \
                                      img.get('attributes', {}).get('data-src') or \
                                      img.get('attributes', {}).get('data-original')
                                if not src and hasattr(img, 'get_attribute'):
                                    src = img.get_attribute('src') or img.get_attribute('data-src') or img.get_attribute('data-original')

                                if src:
                                    image_urls.append(src)
                except Exception as e:
                    print(f"Worker {worker_id}: Warning: Error extracting image URLs: {str(e)}")

                video_urls = []
                
                try:
                    if sb.cdp.is_element_present(f"{content_selector} iframe"):
                        iframe_elements = sb.cdp.find_all(f"{content_selector} iframe", timeout=5)
                        if iframe_elements:
                            for iframe in iframe_elements:
                                src = iframe.get('attributes', {}).get('src')
                                if not src and hasattr(iframe, 'get_attribute'): 
                                     src = iframe.get_attribute('src')
                                if src and src not in video_urls:
                                    if 'youtube.com' in src or 'vimeo.com' in src or 'kickstarter.com/projects/video' in src:
                                         video_urls.append(src)
                except Exception as e:
                    print(f"Worker {worker_id}: Warning: Error extracting iframe video URLs: {str(e)}")

                try:
                     player_selector = ".video-player"
                     if sb.cdp.is_element_present(player_selector):
                         player_elements = sb.cdp.find_all(player_selector, timeout=5)
                         if player_elements:
                              for player in player_elements:
                                  video_url = player.get('attributes', {}).get('data-video-url')
                                  if not video_url and hasattr(player, 'get_attribute'): 
                                       video_url = player.get_attribute('data-video-url')
                                  if video_url and video_url not in video_urls:
                                       video_urls.append(video_url)
                except Exception as e:
                    print(f"Worker {worker_id}: Warning: Error extracting video player URLs: {str(e)}")

                sb.sleep(random.uniform(0.5, 1.5)) 

                if not description_texts and not risk_texts:
                     print(f"Worker {worker_id}: Warning: No description or risk text extracted for {url}.")

                return {
                    'status': 'success',
                    'description': ' '.join(description_texts),
                    'risk': ' '.join(risk_texts),
                    'image_urls': list(set(image_urls)), 
                    'video_urls': list(set(video_urls)), 
                    'image_count': len(list(set(image_urls))),
                    'video_count': len(list(set(video_urls))),
                    'url': url,
                }

        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"
            print(f"Worker {worker_id}: Error on attempt {retry_count + 1}/{max_retries}: {last_error}")
            traceback.print_exc() 
            retry_count += 1
            if retry_count < max_retries:
                retry_delay = random.uniform(3, 7) * retry_count 
                print(f"Worker {worker_id}: Waiting {retry_delay:.2f}s before retrying...")
                time.sleep(retry_delay)
                print(f"Worker {worker_id}: Retrying... (attempt {retry_count + 1}/{max_retries})")

                with rate_limiter_lock:
                     current_requests = rate_limiter.wait_if_needed()
                     print(f"Worker {worker_id}: Current request rate before retry: {current_requests}/{MAX_REQUESTS_PER_MINUTE} per minute")
            else:
                print(f"Worker {worker_id}: Max retries reached for {url}. Logging error.")

        finally:
             if sb and hasattr(sb, 'driver') and sb.driver:
                 try:
                     print(f"Worker {worker_id}: Attempting final cleanup check (should be handled by 'with SB')...")
                 except Exception as quit_err:
                     print(f"Worker {worker_id}: Error during potential final cleanup: {quit_err}")
                 pass

    return {'status': 'error', 'error': last_error, 'url': url}

def load_entries_from_json(json_file_path):
    """Load all entries from a local JSON file

    Args:
        json_file_path (str): Path to the JSON file

    Returns:
        list: All entries from the JSON file
    """
    data = []
    with open(json_file_path, encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data
