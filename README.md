# Kickstarter Project Scraper

This project is designed to scrape detailed information (description, risks, media, metadata) from Kickstarter project pages efficiently and robustly. It uses a combination of parallel processing, proxy management, and automated restarts to handle large numbers of projects and potential network/browser issues.

The system is composed of three main Python scripts:

1.  `single_processor.py`: Handles the core scraping logic for a *single* Kickstarter project page.
2.  `multithread.py`: Orchestrates the parallel scraping of *multiple* projects using `single_processor.py`, managing worker processes and proxies.
3.  `restarter.py`: Manages the lifecycle of `multithread.py`, providing periodic restarts and process cleanup for long-running, stable operation.

## Table of Contents

-   [Overall Workflow](#overall-workflow)
-   [Setup and Installation](#setup-and-installation)
-   [Configuration](#configuration)
-   [Script Breakdown](#script-breakdown)
    -   [`single_processor.py`](#single_processorpy)
    -   [`multithread.py`](#multithreadpy)
    -   [`restarter.py`](#restarterpy)
-   [Execution](#execution)
-   [Output Files](#output-files)
-   [Dependencies](#dependencies)
-   [Error Handling and Robustness](#error-handling-and-robustness)

## Overall Workflow

1.  **Initiation (`restarter.py`)**: The system is typically started by running `restarter.py`.
2.  **Scraper Launch (`restarter.py`)**: `restarter.py` first performs a cleanup of any lingering processes (Chrome, ChromeDriver, proxies) and then starts `multithread.py` as a subprocess.
3.  **Task Distribution (`multithread.py`)**:
    *   `multithread.py` loads the list of project entries from a specified JSON data file (`LOCAL_JSON_FILE`).
    *   It consults the `scraping_checkpoint.json` file to determine which projects have already been processed.
    *   It filters and selects the desired number of unprocessed projects (`TOTAL_ENTRIES`) based on their state ('successful', 'failed', or 'canceled' projects meeting specific criteria).
    *   It populates a shared work queue (`multiprocessing.Manager.Queue`) with the selected project entries.
4.  **Proxy Management (`multithread.py`)**:
    *   The `ProxyManager` class within `multithread.py` starts and manages local `pproxy` instances.
    *   Each worker process is assigned a dedicated local HTTP proxy port. These local proxies forward requests through a list of remote SOCKS5 servers (`PROXY_SERVERS`) provided in the configuration.
    *   `ProxyManager` handles assigning remote proxies to workers, finding available local ports, starting/stopping/restarting `pproxy` subprocesses, and monitoring their health.
5.  **Parallel Processing (`multithread.py`)**:
    *   `multithread.py` spawns a configurable number of worker processes (`NUM_WORKERS`).
    *   Each worker process (`worker_function`) runs independently.
6.  **Individual Scraping (`single_processor.py`)**:
    *   Each worker fetches a project entry from the work queue.
    *   The worker requests its assigned local proxy URL from the `ProxyManager`.
    *   It calls `process_entry_worker` in `single_processor.py`, passing the project entry and the proxy URL.
    *   `single_processor.py` uses `SeleniumBase` (with undetected-chromedriver) to launch a headless Chrome browser instance configured to use the worker's specific local proxy.
    *   It navigates to the project URL, handles potential captchas, waits for dynamic content, and extracts the description, risks, image URLs, and video URLs.
    *   It employs rate limiting (`RateLimiter`) to avoid overwhelming the target site or proxies.
7.  **Data Handling & Checkpointing (`single_processor.py` & `multithread.py`)**:
    *   If scraping is successful, `single_processor.py` returns the extracted data.
    *   `multithread.py` receives the result, formats it with additional metadata from the input entry, and appends it to a shared results list.
    *   The URL is added to a shared list of processed URLs.
    *   Both the results (`project_descriptions.json`) and the checkpoint data (`scraping_checkpoint.json`) are saved to disk using file locks to prevent race conditions.
    *   If scraping fails, the error is logged (`scraping_errors.json`), and the entry might be retried (especially for proxy-related errors, triggering a proxy restart via `ProxyManager`).
8.  **Monitoring (`multithread.py`)**: A separate thread (`progress_reporter`) periodically prints statistics like processed count, error count, queue size, processing rate, and active worker status.
9.  **Restart Cycle (`restarter.py`)**:
    *   `restarter.py` runs in a loop. After a configurable interval (`RESTART_INTERVAL`), it initiates a restart cycle.
    *   It stops the `multithread.py` subprocess.
    *   It performs an extensive cleanup: kills the scraper process, terminates all identified Chrome, ChromeDriver, and `pproxy` processes, and attempts to free all network ports used by the local proxies.
    *   After a waiting period (`WAIT_AFTER_KILL`), it starts a fresh instance of `multithread.py`. This prevents memory leaks, zombie processes, and stale browser/proxy states.
10. **Shutdown**: The system can be stopped gracefully (e.g., via Ctrl+C), triggering signal handlers in `restarter.py` and `multithread.py` to attempt cleanup of proxies and subprocesses. `restarter.py` also uses `atexit` for emergency cleanup.

## Setup and Installation

1.  **Clone the repository.**
2.  **Install Python 3.**
3.  **Install required libraries:**
    ```bash
    pip install seleniumbase undetected-chromedriver pproxy psutil filelock fake-useragent
    ```
    *   `seleniumbase`: Core browser automation and scraping library.
    *   `undetected-chromedriver`: Helps Selenium avoid detection by bot mitigation systems.
    *   `pproxy`: Used to create local HTTP proxies that tunnel traffic through remote SOCKS5 proxies.
    *   `psutil`: Needed by `restarter.py` and `ProxyManager` for process identification and management (finding/killing Chrome, proxies, etc.).
    *   `filelock`: Ensures safe concurrent writing to checkpoint and output files by multiple processes.
    *   `fake-useragent`: (Optional but recommended) Generates realistic User-Agent strings.

## Configuration

Several constants within the Python scripts control the scraper's behavior. Key ones include:

**In `multithread.py`:**

*   `NUM_WORKERS`: Number of parallel scraping processes to launch. Adjust based on CPU cores, memory, and proxy availability.
*   `TOTAL_ENTRIES`: The target number of unique project entries to scrape (including previously scraped ones found in the checkpoint).
*   `PROGRESS_INTERVAL`: How often (in seconds) to print progress updates.
*   `RANDOM_SEED`: Seed for shuffling entries for reproducibility.
*   `LOCAL_JSON_FILE`: **Crucial:** Path to the input JSON file containing Kickstarter project data (one JSON object per line).
*   `PROXY_SERVERS`: **Crucial:** A list of remote SOCKS5 proxy server hostnames or IPs.
*   `PROXY_USERNAME`, `PROXY_PASSWORD`, `PROXY_PORT`: Credentials and port for the SOCKS5 proxies.
*   `BASE_PROXY_PORT`: The starting local port number for the `pproxy` instances. Ensure this range doesn't conflict with other services.

**In `single_processor.py`:**

*   `MAX_REQUESTS_PER_MINUTE`: Rate limit to avoid overwhelming Kickstarter or proxies.
*   `OUTPUT_FILE`: Path to the main output JSON file for successfully scraped data.
*   `CHECKPOINT_FILE`: Path to the checkpoint file tracking progress.
*   `ERROR_LOG_FILE`: Path to the file logging errored entries.

**In `restarter.py`:**

*   `RESTART_INTERVAL`: How often (in seconds) to perform a full restart cycle (stop scraper, cleanup processes/ports, start scraper).
*   `WAIT_AFTER_KILL`: How long (in seconds) to wait after the cleanup phase before restarting the scraper, allowing ports and resources to be fully released.
*   `MULTITHREAD_PATH`: Path to the `multithread.py` script.

## Script Breakdown

### `single_processor.py`

This script focuses on the low-level task of scraping a *single* Kickstarter project page.

*   **Core Function:** `get_description_content(url, entry, checkpoint, results)`
    *   Uses `SeleniumBase` with `uc=True` (undetected-chromedriver) and `headless2=True` for stealthy, efficient headless browsing.
    *   Configures the browser instance to use the specific `local_proxy` provided in the `entry` dictionary.
    *   Sets a random `user_agent` (from `fake-useragent` or a fallback list).
    *   Applies rate limiting via `RateLimiter`.
    *   Navigates to the project `url`. Uses CDP (Chrome DevTools Protocol) commands where possible for robustness (`activate_cdp_mode`, `is_element_present`, etc.).
    *   Includes logic to handle potential captchas (`uc_gui_click_captcha`) and page load errors (`ERR_EMPTY_RESPONSE`).
    *   Waits for key content sections (`.rte__content`, `.js-risks`) to be present.
    *   Extracts text content from description and risks sections, normalizing whitespace.
    *   Extracts `src` attributes from `<img>` tags within the description.
    *   Extracts `src` or `data-video-url` attributes from `<iframe>` and specific video player elements.
    *   Implements a retry mechanism (`max_retries`) for transient errors (network issues, proxy failures, page load timeouts).
    *   Returns a dictionary with `status` ('success', 'skipped', 'error'), extracted data (`description`, `risk`, `image_urls`, `video_urls`), or an `error` message.
*   **Orchestration:** `process_entry_worker(entry, url, checkpoint, results, worker_id)`
    *   Calls `get_description_content`.
    *   If successful:
        *   Loads the latest checkpoint and results using file locks.
        *   Formats the scraped data along with metadata (ID, name, goal, pledged, category, etc.) extracted from the input `entry`.
        *   Appends/updates the result in the `results` list.
        *   Adds the `url` to the `processed_urls` list in the `checkpoint`.
        *   Saves the updated `checkpoint` and `results` using `save_checkpoint_with_verification`.
    *   If an error occurs, logs it using `save_error_entry`.
*   **Persistence:**
    *   `load_checkpoint()`, `save_checkpoint()`, `save_checkpoint_with_verification()`: Use `filelock` to safely read/write `scraping_checkpoint.json` and `project_descriptions.json`. Verification ensures data was written correctly.
    *   `load_error_log()`, `save_error_entry()`: Manage `scraping_errors.json`, also using locks.
*   **Utilities:** `get_random_user_agent()`, `RateLimiter`, `load_entries_from_json()`.

### `multithread.py`

This script manages the parallel execution of scraping tasks using multiple worker processes and handles the complexity of proxy management.

*   **Proxy Management:** `ProxyManager` class
    *   Manages a pool of local `pproxy` subprocesses.
    *   `assign_proxy_hosts_to_workers()`: Maps worker IDs to remote SOCKS5 servers from `PROXY_SERVERS`.
    *   `_find_available_port_for_worker()`: Finds an unused local TCP port within a designated range for each worker.
    *   `_start_new_proxy()`: Starts a `pproxy` subprocess listening on a local port and forwarding to the assigned remote SOCKS5 server with authentication. Includes checks for `pproxy` installation and basic connectivity tests. Uses `psutil` and OS commands (`netstat`) to check/kill processes occupying ports.
    *   `get_proxy_for_worker()`: Provides a worker with its assigned proxy URL, starting/restarting the `pproxy` instance if needed.
    *   `restart_proxy_for_worker()`: Explicitly stops and restarts the proxy for a given worker (used when proxy errors occur).
    *   `stop_all_proxies()`: Terminates all managed `pproxy` instances during shutdown.
    *   Uses `threading.Lock` for safe concurrent access to proxy state.
*   **Worker Logic:** `worker_function(worker_id, ...)`
    *   The main loop for each worker process.
    *   Initializes its proxy using `proxy_manager.get_proxy_for_worker()`.
    *   Continuously fetches project `entry` dictionaries from the shared `work_queue`.
    *   Checks if the entry is already processed or in progress by another worker using the `checkpoint` and a shared `in_progress` list.
    *   Calls `process_entry()`.
    *   Exits when the queue is empty.
*   **Task Processing:** `process_entry(entry, worker_id, ...)`
    *   Acts as a bridge between the worker and `single_processor`.
    *   Retrieves the proxy URL for the worker from `proxy_manager`.
    *   Calls `single_processor.process_entry_worker`.
    *   Handles proxy-related errors returned by `single_processor`. If a proxy error occurs, it requests a proxy restart via `proxy_manager.restart_proxy_for_worker()` and retries the scrape (up to `max_proxy_restarts`).
    *   Updates shared statistics (`shared_stats`) like processed count, errors, and worker status.
*   **Entry Selection:** `get_entries_to_process(total_needed, seed)`
    *   Loads the checkpoint to see how many entries are already done.
    *   Loads all entries from `LOCAL_JSON_FILE`.
    *   Filters entries: Keeps 'successful'/'failed', evaluates 'canceled' projects using `evaluate_canceled_project` (reclassifying some as 'failed'), and excludes already processed ones.
    *   Randomly selects the required number of new entries to reach `total_needed`.
*   **Monitoring:** `progress_reporter(...)`
    *   Runs in a separate thread.
    *   Periodically prints progress (processed/total, errors, rate, queue size, active workers) using data from `shared_stats`.
*   **Main Execution:** `main()`
    *   Sets up `multiprocessing.Manager` to create shared state (dict, queue, list, lock).
    *   Loads and selects entries using `get_entries_to_process`.
    *   Populates the `work_queue`.
    *   Starts the `ProxyManager`.
    *   Spawns worker processes (`multiprocessing.Process`) targeting `worker_function`.
    *   Starts the `progress_reporter` thread.
    *   Waits for worker processes to complete.
    *   Includes signal handling (`SIGINT`, `SIGTERM`) and `atexit` registration for graceful proxy shutdown.

### `restarter.py`

This script acts as a watchdog and process manager for `multithread.py`, ensuring long-term stability by periodically restarting the scraper and cleaning up associated resources.

*   **Core Class:** `DriverRestarter`
    *   `start_multithread()` / `stop_multithread()`: Uses `subprocess.Popen` to run `multithread.py` in a separate process and `process.kill()` to stop it. Captures and logs the scraper's stdout.
    *   `find_chrome_processes()`, `find_python_processes()`, `find_proxy_processes()`: Use `psutil` to scan running processes and identify potentially lingering Chrome, ChromeDriver, related Python scripts (including `pproxy`), and the scraper itself based on name or command line arguments.
    *   `check_used_ports()`, `get_process_using_port()`, `free_port()`, `free_all_proxy_ports()`: Identify processes listening on the ports expected to be used by `pproxy` (based on `BASE_PROXY_PORT` from `multithread.py`). Use `psutil` and OS commands (`netstat`, `taskkill`) to terminate these processes forcefully if needed, ensuring ports are free before the scraper restarts.
    *   `kill_processes()`: Helper function to terminate a list of `psutil.Process` objects gracefully (terminate) and then forcefully (kill).
    *   `restart_cycle()`: Orchestrates the cleanup sequence: stop scraper -> free proxy ports -> kill lingering proxy processes -> kill lingering python processes -> kill lingering chrome/driver processes -> wait -> start scraper.
    *   `run()`: The main loop. Performs an initial cleanup/start, then repeatedly waits for `RESTART_INTERVAL`, triggers `restart_cycle`, and logs progress.
*   **Monitoring:** `ParentProcessMonitor` (Optional)
    *   If needed, can monitor the parent process that launched `restarter.py`. If the parent disappears, it triggers an emergency cleanup.
*   **Cleanup:**
    *   `emergency_cleanup()`: Registered with `atexit`. Attempts to perform a best-effort cleanup (stop scraper, free ports, kill related processes) if the restarter exits unexpectedly.
    *   Signal handling (`SIGINT`, `SIGTERM`, `CTRL_CLOSE_EVENT` on Windows) calls `emergency_cleanup` for graceful shutdown.
*   **Logging:** Uses the standard `logging` module to write detailed logs to `restarter.log` and the console.

## Execution

1.  **Configure** the constants in the scripts, especially `LOCAL_JSON_FILE` and `PROXY_SERVERS` in `multithread.py`.
2.  **Run the restarter script** from your terminal:
    ```bash
    python restarter.py
    ```
3.  `restarter.py` will handle starting, monitoring, and periodically restarting `multithread.py`.
4.  Monitor the console output from both `restarter.py` (for restart cycle info) and `multithread.py` (for scraping progress). Check `restarter.log` for detailed logs.

## Output Files

*   `project_descriptions.json`: The main output file containing successfully scraped data (description, risks, media URLs, metadata) for each project, stored as a JSON list.
*   `scraping_checkpoint.json`: Tracks the progress. Contains a list of `processed_urls`, the total `processed_count`, and `total_processing_time`. Used to resume scraping and avoid redundant work.
*   `scraping_errors.json`: Logs entries that failed to scrape after retries. Contains the original `entry` data, the `url`, the `error` message, and a `timestamp`.
*   `restarter.log`: Detailed log file generated by `restarter.py`, recording startup, shutdown, restart cycles, process cleanup actions, and any errors encountered during the management process.

## Dependencies

*   Python 3.x
*   `seleniumbase`
*   `pproxy`
*   `psutil`
*   `filelock`
*   `fake-useragent` (optional)
