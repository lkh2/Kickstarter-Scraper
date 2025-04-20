import os
import time
import psutil
import signal
import subprocess
import logging
from datetime import datetime
import sys
import platform
import traceback
import atexit
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("restarter.log"),
        logging.StreamHandler(sys.stdout)  
    ]
)

from multithread import BASE_PROXY_PORT

# Constants
RESTART_INTERVAL = 4 * 60 # 4 minutes
WAIT_AFTER_KILL = 1.5 * 60  # 1.5 minutes
MULTITHREAD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "multithread.py") 

def register_process(process, description=None):
    """
    Registers a subprocess object to a global list for potential cleanup.

    Args:
        process: The subprocess object (e.g., from subprocess.Popen).
        description (str, optional): A description of the process. Defaults to "unknown".

    Returns:
        The process object that was passed in.
    """
    global processes_lock, all_processes
    with processes_lock:
        all_processes.append((process, description or "unknown"))
    return process

def is_shutting_down():
    """
    Checks the global flag to see if a shutdown sequence has been initiated.

    Returns:
        bool: True if shutdown is in progress, False otherwise.
    """
    global shutdown_in_progress, shutdown_lock
    with shutdown_lock:
        return shutdown_in_progress

def initiate_shutdown():
    """
    Sets the global flag to indicate that a shutdown sequence should begin.
    """
    global shutdown_in_progress, shutdown_lock
    with shutdown_lock:
        shutdown_in_progress = True
    logging.info("Shutdown initiated")

class DriverRestarter:
    """
    Manages the lifecycle of the parallel scraper script, including periodic
    restarts and cleanup of associated processes (Chrome, ChromeDriver, proxies).
    """
    def __init__(self, multithread_args=None):
        """
        Initializes the DriverRestarter.

        Args:
            multithread_args (list, optional): A list of arguments to pass
                to the multithread.py script. Defaults to None, which results
                in ["--maintain-proxy-mappings"] being used.
        """
        self.multithread_args = multithread_args or []
        if "--maintain-proxy-mappings" not in self.multithread_args:
            self.multithread_args.append("--maintain-proxy-mappings")
        self.process = None
        self.running = True
        self.start_time = datetime.now()
        
    def find_chrome_processes(self):
        """
        Finds all running Chrome and ChromeDriver processes on the system.

        Uses psutil to iterate through processes and identify those matching
        'chrome' or 'chromedriver'/'uc_driver' in their name.

        Returns:
            tuple: A tuple containing two lists:
                - chrome_processes: List of psutil.Process objects for Chrome.
                - driver_processes: List of psutil.Process objects for ChromeDriver/uc_driver.
        """
        chrome_processes = []
        driver_processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if "chrome" in proc.info['name'].lower():
                    chrome_processes.append(proc)

                if "chromedriver" in proc.info['name'].lower() or "uc_driver" in proc.info['name'].lower():
                    driver_processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
                
        return chrome_processes, driver_processes
    
    def find_python_processes(self):
        """
        Finds Python processes potentially related to the scraping operation.

        Identifies Python processes whose command line includes specific keywords
        like 'multithread', 'scrape_batch', 'proxy_', 'restarter',
        'pproxy', or 'selenium'. Excludes the current restarter process.

        Returns:
            list: A list of psutil.Process objects for related Python processes.
        """
        python_processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Get process information
                proc_name = proc.info['name'].lower()
                cmdline = proc.info['cmdline']
                
                # Check if it's a Python process
                is_python = proc_name in ('python.exe', 'python', 'pythonw.exe', 'pythonw')
                
                if is_python and cmdline:
                    cmdline_str = ' '.join(str(cmd) for cmd in cmdline if cmd)
                    
                    # Look for indicators of scraper-related processes
                    if any(term in cmdline_str for term in [
                        'multithread', 'scrape_batch', 'proxy_',
                        'restarter', 'pproxy', 'selenium'
                    ]):
                        # Don't include the current process (restarter)
                        if 'restarter.py' in cmdline_str and proc.pid == os.getpid():
                            continue
                        
                        python_processes.append(proc)
                        logging.info(f"Found related Python process: PID {proc.pid}, CMD: {cmdline_str}")
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                continue
                
        return python_processes
    
    def find_proxy_processes(self):
        """
        Finds processes associated with the proxy service (pproxy).

        Searches for Python processes running 'pproxy' in their command line,
        and also any process with 'proxy' in its name.

        Returns:
            list: A list of psutil.Process objects for proxy-related processes.
        """
        proxy_processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # First check if it's a Python process running pproxy
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(str(cmd) for cmd in cmdline if cmd)
                    
                    # Check for pproxy in command line
                    if 'pproxy' in cmdline_str:
                        proxy_processes.append(proc)
                        logging.info(f"Found pproxy process: PID {proc.pid}, CMD: {cmdline_str}")
                        continue
                
                # Also check for any process with 'proxy' in its name
                proc_name = proc.info['name'].lower()
                if 'proxy' in proc_name:
                    proxy_processes.append(proc)
                    logging.info(f"Found proxy process by name: PID {proc.pid}, Name: {proc_name}")
                    
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                continue
                
        return proxy_processes
    
    def check_used_ports(self, start_port=BASE_PROXY_PORT):
        """
        Checks which ports within the defined proxy range are currently in use.

        Args:
            start_port (int, optional): The starting port number of the range.
                                        Defaults to BASE_PROXY_PORT.
            count (int, optional): The number of ports to check from the start_port.
                                   Defaults to PROXY_PORT_COUNT.

        Returns:
            list: A list of (port, pid) tuples for ports that are currently in use.
        """
        used_ports = []
        
        for port in range(start_port, start_port + 100):
            pid = self.get_process_using_port(port)
            if pid:
                used_ports.append((port, pid))
                logging.info(f"Port {port} is in use by PID {pid}")
                
        return used_ports
    
    def get_process_using_port(self, port):
        """
        Finds the Process ID (PID) of the process using a specific network port.

        Uses platform-specific commands ('netstat' on Windows) to determine
        which process is listening on the given port.

        Args:
            port (int): The port number to check.

        Returns:
            int or None: The PID of the process using the port, or None if the
                         port is not in use or the process cannot be identified.
        """
        try:
            if platform.system() == "Windows":
                cmd = f'netstat -ano | findstr ":{port}"'
                output = subprocess.check_output(cmd, shell=True, text=True)
                
                for line in output.split('\n'):
                    if f":{port}" in line and "LISTENING" in line:
                        parts = line.strip().split()
                        if len(parts) >= 5:
                            return int(parts[4])
        except (subprocess.SubprocessError, ValueError):
            pass
            
        return None
    
    def free_port(self, port):
        """
        Attempts to free a specific network port by terminating the process using it.

        First, it tries to terminate the process gracefully, then forcefully kills it
        if necessary. Includes platform-specific fallbacks (taskkill/netsh on Windows).

        Args:
            port (int): The port number to free.

        Returns:
            bool: True if the port was successfully freed (or was already free),
                  False otherwise.
        """
        pid = self.get_process_using_port(port)
        
        if not pid:
            return True
            
        try:
            process = psutil.Process(pid)
            logging.info(f"Terminating process {pid} using port {port}...")

            try:
                process.terminate()
            except psutil.AccessDenied:
                logging.warning(f"Access denied when terminating PID {pid}, trying with admin privileges")
                if platform.system() == "Windows":
                    subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=False)
                else:
                    subprocess.run(f"kill -15 {pid}", shell=True, check=False)

            try:
                process.wait(timeout=5)
            except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                try:
                    logging.info(f"Process {pid} didn't terminate gracefully, killing it")
                    process.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    if platform.system() == "Windows":
                        subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=False)
                    else:
                        subprocess.run(f"kill -9 {pid}", shell=True, check=False)

                time.sleep(2)

            time.sleep(1) 
            still_in_use = self.get_process_using_port(port)
            
            if still_in_use:
                logging.warning(f"Port {port} is still in use by PID {still_in_use}")
                return False
            else:
                logging.info(f"Port {port} is now free")
                return True
                
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logging.error(f"Error freeing port {port}: {e}")

            try:
                if platform.system() == "Windows":
                    subprocess.run(f"netsh int ipv4 delete excludedportrange protocol=tcp startport={port} numberofports=1", 
                                 shell=True, check=False)
                    subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=False)

                time.sleep(2)
                still_in_use = self.get_process_using_port(port)
                if not still_in_use:
                    logging.info(f"Port {port} freed using platform-specific commands")
                    return True
            except Exception as e2:
                logging.error(f"Failed to free port {port} with platform-specific commands: {e2}")
            
            return False
    
    def free_all_proxy_ports(self):
        """
        Attempts to free all ports within the configured proxy port range.

        Finds and terminates known proxy processes first, then iterates through
        the port range, attempting to free each occupied port using `free_port`.
        Includes an aggressive cleanup phase using platform-specific force kill
        commands if ports remain occupied.
        """
        logging.info(f"Checking and freeing proxy ports in range {BASE_PROXY_PORT} to {BASE_PROXY_PORT + 100}...")

        proxy_processes = self.find_proxy_processes()
        if proxy_processes:
            logging.info(f"Found {len(proxy_processes)} proxy processes to terminate")
            self.kill_processes(proxy_processes)
            time.sleep(3)

        used_ports = self.check_used_ports()
        
        if not used_ports:
            logging.info("No proxy ports in use")
            return
            
        logging.info(f"Found {len(used_ports)} proxy ports in use")
        
        freed_count = 0
        failed_count = 0
        
        for port, pid in used_ports:
            success = self.free_port(port)
            if success:
                freed_count += 1
            else:
                failed_count += 1
                
        logging.info(f"Port cleanup completed: {freed_count} ports freed, {failed_count} ports could not be freed")

        if failed_count > 0:
            logging.info("Some ports could not be freed. Attempting more aggressive cleanup...")
            for port, pid in used_ports:
                try:
                    current_pid = self.get_process_using_port(port)
                    if current_pid:
                        subprocess_cmd = None
                        if platform.system() == "Windows":
                            subprocess_cmd = f"taskkill /F /PID {current_pid}"
                            logging.info(f"Executing force kill: {subprocess_cmd}")
                            subprocess.run(subprocess_cmd, shell=True)
                except Exception as e:
                    logging.error(f"Error in aggressive port cleanup for port {port}: {e}")

            time.sleep(2)

            still_used_ports = self.check_used_ports()
            if still_used_ports:
                logging.warning(f"After aggressive cleanup, still have {len(still_used_ports)} ports in use")
                for port, pid in still_used_ports:
                    logging.warning(f"Port {port} still in use by PID {pid}")
            else:
                logging.info("All ports successfully freed after aggressive cleanup")
    
    def kill_processes(self, processes):
        """
        Attempts to terminate a list of processes gracefully, then forcefully.

        Iterates through the list, first sending a terminate signal, waits briefly,
        and then sends a kill signal to any processes still running.

        Args:
            processes (list): A list of psutil.Process objects to terminate.

        Returns:
            tuple: A tuple containing:
                - terminated (int): The number of processes successfully terminated.
                - killed (int): The number of processes forcefully killed.
        """
        terminated = 0
        killed = 0
        
        for proc in processes:
            try:
                proc.terminate()
                terminated += 1
                logging.info(f"Terminated process {proc.pid} ({proc.info['name']})")
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                logging.warning(f"Failed to terminate {proc.pid}: {e}")

        time.sleep(2)

        for proc in processes:
            try:
                if proc.is_running():
                    proc.kill()
                    killed += 1
                    logging.info(f"Killed process {proc.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
                
        return terminated, killed
    
    def start_multithread(self):
        """
        Starts the multithread.py script as a subprocess.

        Constructs the command, executes it using subprocess.Popen, registers
        the process for cleanup, and starts a thread to log its output.
        Uses creation flags appropriate for Windows to avoid console windows.

        Returns:
            bool: True if the scraper was started successfully, False otherwise.
        """
        if self.process and self.process.poll() is None:
            logging.warning("Parallel scraper is already running")
            return False
                
        try:
            cmd = [sys.executable, MULTITHREAD_PATH] + self.multithread_args
            logging.info(f"Starting parallel scraper with command: {' '.join(cmd)}")

            creation_flags = 0
            if platform.system() == "Windows":
                creation_flags = 0x08000000
            
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                creationflags=creation_flags if platform.system() == "Windows" else 0,
                cwd=os.path.dirname(MULTITHREAD_PATH) 
            )

            register_process(self.process, "multithread")
            
            def log_output():
                for line in self.process.stdout:
                    line = line.strip()
                    if line:  
                        logging.info(f"SCRAPER: {line}")
                        print(f"\033[36mSCRAPER:\033[0m {line}", flush=True)
            
            output_thread = threading.Thread(target=log_output, daemon=True)
            output_thread.start()
            
            logging.info(f"Parallel scraper started with PID {self.process.pid}")
            return True
        except Exception as e:
            logging.error(f"Failed to start parallel scraper: {e}")
            return False
    
    def stop_multithread(self):
        """
        Stops the parallel scraper subprocess if it is currently running.

        Checks if the process exists and is running, then attempts to kill it
        and waits briefly for it to terminate.
        """
        if not self.process:
            return
            
        try:
            if self.process.poll() is None:
                logging.info("Stopping parallel scraper...")
                self.process.kill()
                
                try:
                    self.process.wait(timeout=5)
                    logging.info("Parallel scraper stopped")
                except subprocess.TimeoutExpired:
                    logging.error("Failed to kill parallel scraper process")
        except Exception as e:
            logging.error(f"Error stopping parallel scraper: {e}")
    
    def restart_cycle(self, wait_before_restart=True):
        """
        Performs a complete restart cycle: stop, clean up, wait, start.

        1. Stops the current parallel scraper process.
        2. Frees all proxy ports and terminates associated processes.
        3. Terminates any lingering related Python processes.
        4. Terminates any lingering Chrome and ChromeDriver processes.
        5. Performs a final check and cleanup of proxy ports.
        6. Waits for a defined period (WAIT_AFTER_KILL) if requested.
        7. Starts a new parallel scraper process.

        Args:
            wait_before_restart (bool, optional): If True, waits for WAIT_AFTER_KILL
                seconds before starting the scraper. Defaults to True.
        """
        logging.info("Starting restart cycle")

        self.stop_multithread()

        time.sleep(5)
        logging.info("Brief delay before killing processes to allow file operations to complete")

        self.free_all_proxy_ports()

        proxy_procs = self.find_proxy_processes()
        proxy_count = len(proxy_procs)
        if proxy_procs:
            logging.info(f"Found {proxy_count} proxy processes")
            term_proxy, kill_proxy = self.kill_processes(proxy_procs)
            logging.info(f"Proxy processes: {term_proxy} terminated, {kill_proxy} killed")

        python_procs = self.find_python_processes()
        python_count = len(python_procs)
        if python_procs:
            logging.info(f"Found {python_count} related Python processes")
            term_python, kill_python = self.kill_processes(python_procs)
            logging.info(f"Python processes: {term_python} terminated, {kill_python} killed")

        chrome_procs, driver_procs = self.find_chrome_processes()
        chrome_count = len(chrome_procs)
        driver_count = len(driver_procs)
        
        logging.info(f"Found {chrome_count} Chrome processes and {driver_count} ChromeDriver processes")

        if chrome_procs:
            term_chrome, kill_chrome = self.kill_processes(chrome_procs)
            logging.info(f"Chrome processes: {term_chrome} terminated, {kill_chrome} killed")

        if driver_procs:
            term_driver, kill_driver = self.kill_processes(driver_procs)
            logging.info(f"ChromeDriver processes: {term_driver} terminated, {kill_driver} killed")

        still_used_ports = self.check_used_ports()
        if still_used_ports:
            logging.warning(f"Still have {len(still_used_ports)} ports in use after initial cleanup")
            for port, pid in still_used_ports:
                try:
                    process = psutil.Process(pid)
                    proc_name = process.name()
                    cmdline = " ".join([str(cmd) for cmd in process.cmdline() if cmd])
                    logging.warning(f"Port {port} still used by PID {pid}: {proc_name} - {cmdline}")

                    if platform.system() == "Windows":
                        subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=False)
                except (psutil.NoSuchProcess, psutil.AccessDenied, Exception) as e:
                    logging.error(f"Error handling process on port {port}: {e}")

        if wait_before_restart:
            logging.info(f"Waiting {WAIT_AFTER_KILL} seconds before starting servers again...")
            time.sleep(WAIT_AFTER_KILL)
        else:
            logging.info("Skipping wait period for initial startup")

        success = self.start_multithread()
        if success:
            logging.info("Restart cycle completed successfully")
        else:
            logging.error("Failed to complete restart cycle")
    
    def run(self):
        """
        The main execution loop for the Driver Restarter.

        Performs an initial cleanup and start, then enters a loop that waits
        for the RESTART_INTERVAL, triggers a restart_cycle, and repeats.
        The loop checks for the shutdown flag periodically and exits gracefully
        on KeyboardInterrupt or when the flag is set.
        """
        logging.info("Driver Restarter starting...")

        logging.info("Performing initial cleanup of Chrome processes and proxy ports...")

        self.restart_cycle(wait_before_restart=False)
        
        try:
            while self.running and not is_shutting_down():
                now = datetime.now()
                runtime = (now - self.start_time).total_seconds()
                next_restart = RESTART_INTERVAL - (runtime % RESTART_INTERVAL)
                
                logging.info(f"Next restart in {next_restart:.0f} seconds")

                sleep_interval = 5  
                total_slept = 0
                while total_slept < next_restart and self.running and not is_shutting_down():
                    time.sleep(min(sleep_interval, next_restart - total_slept))
                    total_slept += sleep_interval

                if is_shutting_down() or not self.running:
                    logging.info("Shutdown detected, exiting restart loop")
                    break

                self.restart_cycle(wait_before_restart=True)
                
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, shutting down...")
        finally:
            self.stop_multithread()
            self.free_all_proxy_ports()
            logging.info("Driver Restarter stopped")
    
    def stop(self):
        """
        Stops the Driver Restarter and cleans up resources.

        Sets the running flag to False, stops the parallel scraper process,
        and frees all proxy ports.
        """
        self.running = False
        self.stop_multithread()
        self.free_all_proxy_ports()

class ParentProcessMonitor:
    """
    Monitors the parent process and triggers an emergency cleanup if the parent disappears.

    This is useful when running the restarter as a child process, ensuring cleanup
    happens even if the parent is terminated unexpectedly.
    """
    def __init__(self):
        """Initializes the ParentProcessMonitor."""
        self.running = True
        self.parent_pid = os.getppid()
        self.monitor_thread = None
        
    def start_monitoring(self):
        """
        Starts the monitoring process in a separate daemon thread.

        Returns:
            threading.Thread: The thread object running the monitor loop.
        """
        import threading
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self.monitor_thread.start()
        logging.info(f"Started monitoring parent process (PID: {self.parent_pid})")
        return self.monitor_thread
        
    def _monitor_loop(self):
        """
        The core loop that periodically checks if the parent process is still running.

        If the parent process is found to be gone, it initiates the global shutdown
        and calls the emergency cleanup function.
        """
        while self.running and not is_shutting_down():
            if not self._is_parent_running():
                logging.info(f"Parent process (PID: {self.parent_pid}) is gone. Initiating shutdown...")
                initiate_shutdown()
                emergency_cleanup() 
                os._exit(0) 
            
            time.sleep(5) 
            
    def _is_parent_running(self):
        """
        Checks if the parent process identified at initialization is still running.

        Uses psutil to check the status of the parent PID. Handles potential
        errors like the process already being gone.

        Returns:
            bool: True if the parent process is running, False otherwise.
        """
        try:
            parent = psutil.Process(self.parent_pid)
            return parent.is_running() 
        except (ProcessLookupError, psutil.NoSuchProcess):
            return False
        except Exception as e:
            logging.error(f"Error checking parent process: {e}")
            return False
    
    def stop(self):
        """Stops the monitoring thread."""
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2) 

def emergency_cleanup():
    """
    Performs an emergency cleanup of processes and resources.

    This function is registered with atexit and called during unexpected
    termination or when the parent process disappears. It attempts to stop
    the scraper, free ports, and kill related processes.
    """
    if is_shutting_down(): 
        return
        
    logging.info("Starting emergency cleanup...")
    initiate_shutdown() 
    
    try:
        restarter = DriverRestarter()
        restarter.stop_multithread()
        restarter.free_all_proxy_ports()

        python_procs = restarter.find_python_processes()
        if python_procs:
            logging.info(f"Emergency cleanup: Found {len(python_procs)} Python processes to terminate")
            restarter.kill_processes(python_procs)
        
        chrome_procs, driver_procs = restarter.find_chrome_processes()
        if chrome_procs or driver_procs:
            logging.info(f"Emergency cleanup: Found {len(chrome_procs)} Chrome and {len(driver_procs)} driver processes")
            if chrome_procs:
                restarter.kill_processes(chrome_procs)
            if driver_procs:
                restarter.kill_processes(driver_procs)
    except Exception as e:
        logging.error(f"Error during emergency cleanup: {e}")
        traceback.print_exc()

    global processes_lock, all_processes
    with processes_lock:
        processes_to_kill = all_processes.copy() 
        
    for process, description in processes_to_kill:
        try:
            if hasattr(process, "poll") and process.poll() is None:
                logging.info(f"Force killing registered process: {description} (PID: {process.pid if hasattr(process, 'pid') else 'N/A'})")
                process.kill()
        except Exception as e:
            logging.error(f"Error killing registered process {description}: {e}")
    
    logging.info("Emergency cleanup completed")

# Global flag for shutdown
shutdown_in_progress = False
shutdown_lock = None  

# Track all created processes for emergency cleanup
all_processes = []  
processes_lock = None  

if __name__ == "__main__":
    import threading
    shutdown_lock = threading.Lock()
    processes_lock = threading.Lock()
    
    atexit.register(emergency_cleanup)

    print("\n" + "=" * 50)
    print("DRIVER RESTARTER STARTING")
    print("=" * 50 + "\n")

    logging.info(f"Starting Driver Restarter with interval: {RESTART_INTERVAL}s, wait time: {WAIT_AFTER_KILL}s")
    parallel_args_to_pass = []
    logging.info(f"Default parallel scraper args being used: {['--maintain-proxy-mappings']}")

    restarter = DriverRestarter(multithread_args=parallel_args_to_pass)

    def signal_handler(sig, frame):
        """Handles termination signals (SIGINT, SIGTERM, etc.) for graceful shutdown."""
        print("\n" + "=" * 50)
        print("SHUTTING DOWN DRIVER RESTARTER")
        print("=" * 50)
        logging.info(f"Signal {sig} received, shutting down...")
        initiate_shutdown()
        restarter.stop()
        emergency_cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if platform.system() == "Windows":
        try:
            signal.signal(signal.CTRL_CLOSE_EVENT, signal_handler)
        except (AttributeError, ValueError):
            logging.warning("Could not register CTRL_CLOSE_EVENT handler")
    
    parent_monitor = ParentProcessMonitor()
    parent_monitor.start_monitoring()

    try:
        restarter.run()
    except Exception as e:
        logging.error(f"Unhandled exception in main execution: {e}")
        traceback.print_exc()
    finally:
        initiate_shutdown()
        restarter.stop()
        emergency_cleanup()
        parent_monitor.stop()
