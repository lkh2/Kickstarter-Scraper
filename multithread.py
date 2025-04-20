import multiprocessing
import time
import random
from datetime import datetime
import threading
import queue
import os
import subprocess
import signal
import atexit
import psutil
import socket
import sys
import traceback

from single_processor import (
    process_entry_worker, load_entries_from_json, load_checkpoint
)

NUM_WORKERS = 10 # Number of worker processes to spawn
TOTAL_ENTRIES = 20000 # Total number of entries to process
PROGRESS_INTERVAL = 60 # Interval in seconds to print progress
RANDOM_SEED = 42 # Seed for random number generation (for reproducibility)
LOCAL_JSON_FILE = r"" # Path to the JSON file containing entries to process

LOCAL_PROXY_HOST = "127.0.0.1" # Local host for pproxy
BASE_PROXY_PORT = 8899  # Base port for local pproxy instances

PROXY_USERNAME = ""  # Username for SOCKS5 proxy authentication (if needed)
PROXY_PASSWORD = ""  # Password for SOCKS5 proxy authentication (if needed)
PROXY_PORT = "1080" # Default SOCKS5 port
PROXY_SERVERS = [
    # Example proxy servers (replace with actual ones)
    # "amsterdam.nl.socks.nordhold.net",
    # "atlanta.us.socks.nordhold.net",
    # "dallas.us.socks.nordhold.net",
    # "los-angeles.us.socks.nordhold.net",
    # "nl.socks.nordhold.net",
    # "se.socks.nordhold.net",
    # "stockholm.se.socks.nordhold.net",
    # "us.socks.nordhold.net",
    # "new-york.us.socks.nordhold.net",
    # "san-francisco.us.socks.nordhold.net",
    # "detroit.us.socks.nordhold.net"
]

class ProxyManager:
    """Manages multiple local pproxy instances, each forwarding to a SOCKS5 proxy.

    Handles assigning ports and remote SOCKS5 servers to workers, starting,
    stopping, restarting, and monitoring the local proxy processes. It ensures
    each worker process uses a dedicated local proxy port linked to a potentially
    shared remote SOCKS5 server.
    """
    def __init__(self):
        """Initializes the ProxyManager."""
        self.proxies = {}  # Dictionary of worker_id -> proxy info
        self.proxy_lock = threading.Lock()
        self.port_map = {}  # Dictionary mapping worker_id -> port
        self.used_ports = set()  # Set of all ports currently in use
        self.port_range_size = 10  # Each worker gets this many potential ports
        self.worker_proxy_map = {}  # Dictionary mapping worker_id -> proxy_host

    def assign_proxy_hosts_to_workers(self, num_workers):
        """Assigns dedicated remote SOCKS5 proxy hosts to each worker ID sequentially.

        Args:
            num_workers (int): The total number of workers to assign proxies for.
        """
        with self.proxy_lock:
            print("Creating dedicated proxy-worker mappings...")
            if not PROXY_SERVERS:
                 print("ERROR: PROXY_SERVERS list is empty. Cannot assign proxy hosts.")
                 return

            for worker_id in range(num_workers):
                proxy_index = worker_id % len(PROXY_SERVERS)
                proxy_host = PROXY_SERVERS[proxy_index]
                self.worker_proxy_map[worker_id] = proxy_host
                print(f"Worker {worker_id} assigned to proxy: {proxy_host}")

    def _get_port_range_for_worker(self, worker_id):
        """Calculates the designated range of local ports for a given worker.

        Args:
            worker_id (int): The ID of the worker.

        Returns:
            tuple[int, int]: A tuple containing the start and end port for the worker.
        """
        start_port = BASE_PROXY_PORT + (worker_id * self.port_range_size)
        end_port = start_port + self.port_range_size - 1
        return (start_port, end_port)

    def _find_available_port_for_worker(self, worker_id):
        """Finds an available local TCP port for a worker to bind its proxy.

        Searches within the worker's designated range first. If a cached port
        exists and is available, it's reused. If the primary range is full,
        it attempts to find a port in a fallback range.

        Args:
            worker_id (int): The ID of the worker.

        Returns:
            int | None: An available port number, or None if no port could be found.
        """
        start_port, end_port = self._get_port_range_for_worker(worker_id)
        print(f"Worker {worker_id}: Searching for available port in range {start_port}-{end_port}")

        if worker_id in self.port_map:
            cached_port = self.port_map[worker_id]
            if cached_port not in self.used_ports and self._is_port_available(cached_port):
                 print(f"Worker {worker_id}: Reusing cached available port {cached_port}")
                 return cached_port

        for port in range(start_port, end_port + 1):
            if port not in self.used_ports and self._is_port_available(port):
                print(f"Worker {worker_id}: Found available primary port {port}")
                return port

        fallback_base = BASE_PROXY_PORT + (NUM_WORKERS * self.port_range_size) + 100 
        max_fallback_attempts = 50 
        for offset in range(max_fallback_attempts):
            fallback_port = fallback_base + (worker_id * max_fallback_attempts) + offset
            if fallback_port not in self.used_ports and self._is_port_available(fallback_port):
                print(f"Worker {worker_id}: Found available fallback port {fallback_port}")
                return fallback_port

        print(f"Worker {worker_id}: FATAL - No available ports found for proxy restart.")
        return None

    def _is_port_available(self, port):
        """Checks if a local TCP port is available by attempting to bind to it.

        Args:
            port (int): The port number to check.

        Returns:
            bool: True if the port is available, False otherwise.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5) 
        try:
            sock.bind((LOCAL_PROXY_HOST, port))
            available = True
        except socket.error as e:
            import errno
            if e.errno == errno.EADDRINUSE or e.errno == 10048:
                 pass 
                 print(f"Port {port} check: In use ({e.strerror})")
            else:
                 print(f"Port {port} check: Unexpected socket error: {e}")
            available = False
        except Exception as e:
             print(f"Port {port} check: General error: {e}")
             available = False
        finally:
            sock.close()
        return available

    def _get_process_using_port(self, port):
        """Identifies the process ID (PID) currently listening on a given local port using psutil.

        Falls back to OS-specific commands if psutil fails or lacks permissions.

        Args:
            port (int): The port number to check.

        Returns:
            int | None | -1: The PID of the process using the port, None if the port
                             is free, or -1 if the PID could not be determined
                             (e.g., due to permissions or fallback failure).
        """
        try:
             for conn in psutil.net_connections(kind='inet'):
                  if conn.status == psutil.CONN_LISTEN and conn.laddr.port == port:
                       if conn.pid:
                           print(f"Port {port} is used by PID {conn.pid}")
                           return conn.pid
                       else:
                           print(f"Warning: Port {port} is in use, but PID could not be determined.")
                           return -1 
        except psutil.AccessDenied:
             print("Warning: Access denied while checking network connections. Cannot determine process using port.")
             return self._get_process_using_port_fallback(port)
        except Exception as e:
             print(f"Error checking port {port}: {e}")
             return self._get_process_using_port_fallback(port)

        return None 

    def _get_process_using_port_fallback(self, port):
         """Fallback method to find the process using a port via OS commands (netstat).

         Used when psutil fails (e.g., due to permissions).

         Args:
             port (int): The port number to check.

         Returns:
             int | None: The PID if found, otherwise None.
         """
         print(f"Attempting fallback check for port {port}...")
         pid = None
         try:
              if os.name == "nt": 
                   cmd = f'netstat -ano | findstr ":{port}" | findstr "LISTENING"'
                   output = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
                   for line in output.strip().split('\n'):
                        if line:
                             parts = line.strip().split()
                             if len(parts) >= 5:
                                  pid = int(parts[-1])
                                  print(f"Fallback: Port {port} used by PID {pid} (netstat)")
                                  break 

         except (subprocess.SubprocessError, ValueError) as e:
              pass
         except FileNotFoundError:
              print(f"Fallback command not found (netstat/lsof/ss). Cannot determine process using port {port}.")
         except Exception as e:
              print(f"Direct proxy connection test to {port} failed: {type(e).__name__} - {str(e)}")
              return False

         return pid

    def _kill_process_using_port(self, port):
        """Attempts to terminate the process currently using the specified port.

        First tries a graceful termination (SIGTERM), then resorts to killing
        (SIGKILL) if necessary. Verifies the port is free afterwards.

        Args:
            port (int): The port number to free.

        Returns:
            bool: True if the port is free (or was already free), False if the
                  process could not be terminated or the port remains in use.
        """
        pid = self._get_process_using_port(port)

        if pid is None:
            return True
        if pid == -1:
             print(f"Cannot kill process using port {port}: PID unknown.")
             return False 

        try:
            print(f"Attempting to terminate process {pid} using port {port}")
            process = psutil.Process(pid)

            try:
                 process.terminate()
                 print(f"Sent SIGTERM to PID {pid}")
            except psutil.NoSuchProcess:
                 print(f"Process {pid} already terminated.")
                 return True 
            except psutil.AccessDenied:
                 print(f"Access denied for terminating PID {pid}, will try kill.")
                 pass 

            try:
                 process.wait(timeout=2)
                 print(f"Process {pid} terminated gracefully.")
                 time.sleep(0.5)
                 if self._get_process_using_port(port) is None:
                      print(f"Port {port} is now free.")
                      return True
                 else:
                      print(f"Warning: Process {pid} still in use after kill.")
            except psutil.TimeoutExpired:
                 print(f"Process {pid} did not terminate gracefully, attempting kill.")
            except psutil.NoSuchProcess:
                 print(f"Process {pid} disappeared after terminate signal.")
                 return True

            try:
                 print(f"Sending SIGKILL to PID {pid}")
                 process.kill()
                 process.wait(timeout=1) 
                 print(f"Process {pid} killed.")
            except psutil.NoSuchProcess:
                 print(f"Process {pid} disappeared after kill signal.")
            except (psutil.AccessDenied, Exception) as kill_err:
                 print(f"Failed to kill process {pid} with psutil: {kill_err}")
                 return False

            time.sleep(1) 
            if self._get_process_using_port(port) is None:
                print(f"Port {port} is now free after kill attempts.")
                return True
            else:
                print(f"CRITICAL: Port {port} STILL in use after killing PID {pid}.")
                return False

        except psutil.NoSuchProcess:
            print(f"Process {pid} does not exist (already gone). Port should be free.")
            return True 
        except Exception as e:
            print(f"Error freeing port {port} (PID {pid}): {e}")
            return False

    def get_proxy_for_worker(self, worker_id):
        """Retrieves or creates a local proxy instance for a specific worker.

        Checks if a valid proxy process already exists for the worker. If not,
        or if the existing process has died, it finds an available port and
        starts a new pproxy instance, retrying a few times if the initial start fails.

        Args:
            worker_id (int): The ID of the worker requesting a proxy.

        Returns:
            dict | None: A dictionary containing proxy information ('url', 'process',
                         'host', 'port', etc.) or None if a proxy could not be
                         provided after multiple attempts.
        """
        with self.proxy_lock:
            if worker_id in self.proxies and self.proxies[worker_id]:
                proxy_info = self.proxies[worker_id]
                process = proxy_info["process"]
                port = proxy_info.get("port")
                if process and process.poll() is None and self.port_map.get(worker_id) == port:
                    return proxy_info
                else:
                    print(f"Worker {worker_id}: Proxy process died or port mapping changed. Recreating.")
                    if port in self.used_ports:
                         self.used_ports.remove(port)
                    if worker_id in self.proxies:
                        del self.proxies[worker_id]
                    if worker_id in self.port_map:
                         del self.port_map[worker_id]

            port = self._find_available_port_for_worker(worker_id)
            if port is None:
                print(f"Worker {worker_id}: No available ports found!")
                return None

            max_attempts = 3 
            for attempt in range(1, max_attempts + 1):
                print(f"Worker {worker_id}: Attempt {attempt}/{max_attempts} to create proxy on port {port}...")
                proxy_info = self._start_new_proxy(worker_id, port)

                if proxy_info:
                    self.used_ports.add(port)
                    self.port_map[worker_id] = port
                    self.proxies[worker_id] = proxy_info
                    print(f"Worker {worker_id}: Successfully started proxy on port {port}.")
                    return proxy_info

                if not self._is_port_available(port):
                     print(f"Worker {worker_id}: Port {port} became unavailable after failed start attempt.")
                     if attempt < max_attempts:
                          print(f"Worker {worker_id}: Finding a new port for retry...")
                          new_port = self._find_available_port_for_worker(worker_id)
                          if new_port and new_port != port:
                               port = new_port 
                          else:
                               print(f"Worker {worker_id}: Could not find a new port. Aborting proxy creation.")
                               return None

                if attempt < max_attempts:
                    wait_time = 5
                    print(f"Worker {worker_id}: Proxy creation failed, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

            print(f"Worker {worker_id}: FATAL - Failed to create proxy on port {port} after {max_attempts} attempts")
            if port in self.used_ports:
                 self.used_ports.remove(port)
            if worker_id in self.port_map and self.port_map[worker_id] == port:
                 del self.port_map[worker_id]
            return None

    def _get_proxy_host_for_worker(self, worker_id):
        """Gets the assigned remote SOCKS5 proxy host for a specific worker.

        Uses the pre-assigned mapping if available, otherwise assigns one dynamically
        (though ideally `assign_proxy_hosts_to_workers` is called first).

        Args:
            worker_id (int): The ID of the worker.

        Returns:
            str: The hostname of the remote SOCKS5 proxy server.

        Raises:
            ValueError: If no proxy servers are configured at all.
        """
        if not self.worker_proxy_map:
             print("Warning: Worker-proxy map is empty. Assigning random proxy.")
             if not PROXY_SERVERS: raise ValueError("No proxy servers configured")
             return random.choice(PROXY_SERVERS)

        if worker_id in self.worker_proxy_map:
            proxy_host = self.worker_proxy_map[worker_id]
            return proxy_host
        else:
            print(f"Warning: Worker {worker_id} not found in proxy map. Assigning dynamically.")
            proxy_host = random.choice(PROXY_SERVERS)
            self.worker_proxy_map[worker_id] = proxy_host
            return proxy_host

    def _start_new_proxy(self, worker_id, port):
        """Starts a new pproxy process on the specified local port for a worker.

        Configures pproxy to listen locally and forward to the worker's assigned
        remote SOCKS5 server with authentication. It attempts to free the port
        if occupied, checks if pproxy is installed, monitors the process startup,
        and tests connectivity.

        Args:
            worker_id (int): The ID of the worker.
            port (int): The local port to bind the pproxy instance to.

        Returns:
            dict | None: A dictionary with proxy details ('url', 'process', etc.)
                         if successful, otherwise None.
        """
        if not self._is_port_available(port):
            print(f"Worker {worker_id}: Port {port} is in use. Attempting to kill owner...")
            if not self._kill_process_using_port(port):
                print(f"Worker {worker_id}: Failed to free port {port}. Cannot start proxy.")
                return None
            else:
                print(f"Worker {worker_id}: Port {port} freed. Waiting briefly...")
                time.sleep(1.5)

        if not self._is_port_available(port):
            print(f"Worker {worker_id}: Port {port} still in use after cleanup. Aborting proxy start.")
            return None

        try:
            proxy_host = self._get_proxy_host_for_worker(worker_id)
        except Exception as e:
            print(f"Worker {worker_id}: Error getting proxy host: {str(e)}")
            return None

        local_http_proxy_arg = f"http://{LOCAL_PROXY_HOST}:{port}"
        escaped_password = PROXY_PASSWORD.replace('#', '\\#')
        remote_server_arg = f"socks5://{proxy_host}:{PROXY_PORT}#{PROXY_USERNAME}:{escaped_password}"
        proxy_url = f"http://{LOCAL_PROXY_HOST}:{port}"

        print(f"Worker {worker_id}: Starting pproxy: {local_http_proxy_arg} -> {proxy_host}:{PROXY_PORT}")

        try:
             subprocess.run([sys.executable, "-m", "pproxy", "--version"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        except (subprocess.SubprocessError, FileNotFoundError):
             print(f"Worker {worker_id}: pproxy not found or check failed. Attempting installation...")
             try:
                  install_result = subprocess.run(
                       [sys.executable, "-m", "pip", "install", "pproxy"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True
                  )
                  print(f"Worker {worker_id}: Successfully installed pproxy.")
             except subprocess.CalledProcessError as e:
                  print(f"Worker {worker_id}: FATAL - Failed to install pproxy: {e.stderr}")
                  return None
             except Exception as e:
                  print(f"Worker {worker_id}: FATAL - Error during pproxy installation: {str(e)}")
                  return None

        cmd = [
            sys.executable, "-m", "pproxy",
            "-l", local_http_proxy_arg,
            "-r", remote_server_arg
        ]

        process = None
        try:
            creation_flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, 
                universal_newlines=True,
                bufsize=1, 
                creationflags=creation_flags
            )

            print(f"Worker {worker_id}: Waiting for proxy on port {port} to initialize...")
            max_wait_time = 15 
            start_time = time.time()
            initialized = False
            while time.time() - start_time < max_wait_time:
                if process.poll() is not None:
                    output = process.communicate()[0] 
                    print(f"Worker {worker_id}: pproxy process terminated unexpectedly during startup (port {port}). Exit code: {process.returncode}")
                    print(f"--- pproxy output ---:\n{output}\n---------------------")
                    return None

                if self._can_connect_to_proxy(proxy_url):
                    print(f"Worker {worker_id}: Proxy on port {port} is connectable.")
                    initialized = True
                    break

                time.sleep(0.5) 

            if not initialized:
                 print(f"Worker {worker_id}: Proxy on port {port} did not become connectable within {max_wait_time}s.")
                 try: process.kill()
                 except: pass
                 return None

            monitor_thread = threading.Thread(
                 target=self._monitor_proxy_output,
                 args=(process, worker_id, port),
                 daemon=True,
                 name=f"ProxyMonitor-{worker_id}-{port}"
            )
            monitor_thread.start()

            test_result = self._test_direct_proxy_connection(proxy_host, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD)
            if test_result:
                 pass
            else:
                 print(f"Worker {worker_id}: Warning - Direct SOCKS5 connection test to {proxy_host} failed. Proxy *might* still work.")


            proxy_info = {
                "url": proxy_url,
                "process": process,
                "socks5_proxy": remote_server_arg, 
                "host": proxy_host,
                "port": port,
                "test_result": test_result, 
                "start_time": time.time()
            }
            return proxy_info

        except Exception as e:
            print(f"Worker {worker_id}: Error starting pproxy on port {port}: {str(e)}")
            if process and process.poll() is None:
                try: process.kill()
                except: pass
            return None


    def _can_connect_to_proxy(self, proxy_url):
        """Checks if a TCP connection can be established to the local HTTP proxy URL.

        Args:
            proxy_url (str): The local proxy URL (e.g., "http://127.0.0.1:8899").

        Returns:
            bool: True if a connection can be made within the timeout, False otherwise.
        """
        try:
            parts = proxy_url.replace("http://", "").split(":")
            if len(parts) != 2: return False
            host, port_str = parts
            port = int(port_str)

            with socket.create_connection((host, port), timeout=1) as sock:
                 return True 
        except (socket.timeout, ConnectionRefusedError, ValueError, IndexError):
            return False
        except Exception as e:
             return False

    def restart_proxy_for_worker(self, worker_id):
        """Stops the existing proxy (if any) and starts a new one for the worker.

        Attempts to reuse the worker's previous port if it's available, otherwise
        finds a new one.

        Args:
            worker_id (int): The ID of the worker whose proxy needs restarting.

        Returns:
            dict | None: The new proxy information dictionary if successful, else None.
        """
        with self.proxy_lock:
             print(f"Worker {worker_id}: Received request to restart proxy.")
             old_proxy_info = self.proxies.pop(worker_id, None)
             old_port = self.port_map.pop(worker_id, None)

             if old_proxy_info and old_proxy_info.get("process"):
                  old_process = old_proxy_info["process"]
                  print(f"Worker {worker_id}: Stopping old proxy process (PID {old_process.pid if hasattr(old_process, 'pid') else 'N/A'}) on port {old_port}...")
                  try:
                       if old_process.poll() is None:
                           old_process.terminate()
                           old_process.wait(timeout=3)
                  except Exception as e:
                       print(f"Worker {worker_id}: Error terminating old proxy: {e}. Attempting kill...")
                       try:
                            if old_process.poll() is None: old_process.kill()
                       except Exception as kill_e:
                            print(f"Worker {worker_id}: Error killing old proxy: {kill_e}")

             if old_port in self.used_ports:
                  self.used_ports.remove(old_port)

             port_to_use = None
             if old_port and self._is_port_available(old_port):
                  print(f"Worker {worker_id}: Reusing previous port {old_port} for restart.")
                  port_to_use = old_port
             else:
                  if old_port:
                      print(f"Worker {worker_id}: Previous port {old_port} unavailable. Finding new port...")
                  port_to_use = self._find_available_port_for_worker(worker_id)

             if port_to_use is None:
                  print(f"Worker {worker_id}: FATAL - No available ports found for proxy restart.")
                  return None

             print(f"Worker {worker_id}: Starting new proxy instance on port {port_to_use}...")
             new_proxy_info = self._start_new_proxy(worker_id, port_to_use)

             if new_proxy_info:
                  self.proxies[worker_id] = new_proxy_info
                  self.port_map[worker_id] = port_to_use
                  self.used_ports.add(port_to_use)
                  return new_proxy_info
             else:
                  print(f"Worker {worker_id}: FATAL - Failed to restart proxy after stopping old one.")
                  if port_to_use in self.used_ports:
                       self.used_ports.remove(port_to_use)
                  return None

    def get_port_for_worker(self, worker_id):
        """Gets the local port currently assigned to the specified worker.

        Args:
            worker_id (int): The worker ID.

        Returns:
            int | None: The port number, or None if no port is currently assigned.
        """
        with self.proxy_lock:
            return self.port_map.get(worker_id)

    def stop_all_proxies(self):
        """Attempts to terminate all managed pproxy processes gracefully."""
        print("Stopping all managed proxy servers...")
        with self.proxy_lock:
             stopped_count = 0
             for worker_id, proxy_info in list(self.proxies.items()):
                  if proxy_info and "process" in proxy_info:
                       process = proxy_info["process"]
                       port = proxy_info.get("port", "N/A")
                       print(f"Stopping proxy for worker {worker_id} (Port: {port}, PID: {process.pid if hasattr(process,'pid') else 'N/A'})...")
                       try:
                           if process.poll() is None: 
                               process.terminate()
                               process.wait(timeout=3) 
                               stopped_count += 1
                       except Exception as e:
                           print(f"Error stopping proxy for worker {worker_id}: {str(e)}. Attempting kill...")
                           try:
                                if process.poll() is None: process.kill()
                           except Exception as kill_e:
                                print(f"Error killing proxy for worker {worker_id}: {kill_e}")

             print(f"Attempted to stop {len(self.proxies)} proxies, {stopped_count} were running.")
             self.proxies.clear()
             self.port_map.clear()
             self.used_ports.clear()
             self.worker_proxy_map.clear() 
             print("Proxy manager state cleared.")

    def _monitor_proxy_output(self, process, worker_id, port):
        """Monitors the stdout/stderr of a pproxy process in a separate thread.

        Prints lines containing error-related keywords with context.

        Args:
            process (subprocess.Popen): The pproxy process object.
            worker_id (int): The ID of the worker associated with this proxy.
            port (int): The local port the proxy is running on.
        """
        try:
             for line in iter(process.stdout.readline, ''):
                  line = line.strip()
                  if line:
                       lower_line = line.lower()
                       if "error" in lower_line or "exception" in lower_line or "fail" in lower_line or "traceback" in lower_line:
                           print(f"PPROXY ERROR [W:{worker_id}|P:{port}]: {line}")
                       elif "listen " in lower_line or "forward " in lower_line or " started" in lower_line:
                           pass
             process.wait() 
             if process.returncode != 0 and process.returncode is not None:
                  print(f"PPROXY WARN [W:{worker_id}|P:{port}]: Process exited with code {process.returncode}")

        except ValueError:
             if process.poll() is None:
                  print(f"PPROXY WARN [W:{worker_id}|P:{port}]: ValueError reading proxy output, process might still be running?")
        except Exception as e:
             print(f"PPROXY ERROR [W:{worker_id}|P:{port}]: Error monitoring proxy output: {str(e)}")


    def _test_direct_proxy_connection(self, host, port, username, password):
        """Performs a basic TCP connection test to the remote SOCKS5 proxy server.

        Note: This only checks reachability, not full SOCKS5 protocol negotiation.

        Args:
            host (str): The remote SOCKS5 proxy hostname or IP.
            port (str | int): The remote SOCKS5 proxy port.
            username (str): SOCKS5 username (currently unused in test).
            password (str): SOCKS5 password (currently unused in test).

        Returns:
            bool: True if a TCP connection was established, False otherwise.
        """
        print(f"Testing direct SOCKS5 connection to {host}:{port}...")
        try:
             try:
                  proxy_ip = socket.gethostbyname(host)
             except socket.gaierror as e:
                  print(f"Failed to resolve proxy host {host}: {e}")
                  return False

             with socket.create_connection((host, int(port)), timeout=5) as s:
                  return True 

        except socket.timeout:
             print(f"Failed to connect to proxy {host}:{port}: Connection timed out")
             return False
        except ConnectionRefusedError:
             print(f"Failed to connect to proxy {host}:{port}: Connection refused")
             return False
        except Exception as e:
             print(f"Direct proxy connection test to {host}:{port} failed: {type(e).__name__} - {str(e)}")
             return False

proxy_manager = ProxyManager()

def process_entry(entry, worker_id, shared_stats, status_lock, work_queue, in_progress):
    """Processes a single Kickstarter project entry using a dedicated worker context.

    Acquires a proxy for the worker, calls the core processing function
    (`single_processor.process_entry_worker`), handles potential proxy errors
    by attempting proxy restarts, and updates shared statistics.

    Args:
        entry (dict): The dictionary representing the Kickstarter project entry.
        worker_id (int): The ID of the worker process handling this entry.
        shared_stats (multiprocessing.Manager.dict): Shared dictionary for statistics.
        status_lock (multiprocessing.Manager.Lock): Lock for accessing shared_stats.
        work_queue (multiprocessing.Manager.Queue): The queue to potentially re-add failed items (not currently used).
        in_progress (multiprocessing.Manager.list): Shared list of URLs currently being processed.

    Returns:
        dict: A result dictionary containing 'status' ('success', 'error', 'skipped'),
              'url', and potentially 'error' message.
    """
    url = entry['data']['urls']['web']['project']
    entry_id = entry['data']['id']
    worker_str = f"Worker {worker_id}: "

    with status_lock:
        shared_stats['worker_status'][worker_id] = {
            'url': url,
            'start_time': time.time(),
            'status': 'processing'
        }

    proxy_info = None
    try:
        proxy_info = proxy_manager.get_proxy_for_worker(worker_id)
        if not proxy_info:
            print(f"{worker_str}FATAL - Failed to get proxy for {url}. Skipping.")
            raise Exception("Failed to acquire proxy")
    except Exception as e:
        print(f"{worker_str}Error acquiring proxy for {url}: {e}")
        with status_lock:
            shared_stats['errors'] += 1
            shared_stats['worker_status'][worker_id]['status'] = 'error (proxy)'
            if url in in_progress: in_progress.remove(url)
        return {'status': 'error', 'error': f'Proxy acquisition failed: {e}', 'url': url}

    modified_entry = entry.copy()
    modified_entry['local_proxy'] = proxy_info['url']
    modified_entry['worker_id'] = worker_id

    result = None
    max_proxy_restarts = 2
    proxy_restart_count = 0
    last_error_message = "No attempt made"

    while proxy_restart_count <= max_proxy_restarts:
        checkpoint, results = load_checkpoint()

        result = process_entry_worker(
            modified_entry,
            url,
            checkpoint,
            results,
            worker_id=worker_id
        )
        last_error_message = result.get('error', 'Unknown status') if result else 'None result'

        if result and result.get('status') == 'success':
             break

        elif result and result.get('status') == 'skipped':
             break

        elif result and result.get('status') == 'empty':
             print(f"{worker_str}Warning: Empty content for {url}. Not retrying.")
             break

        else:
            error_message = result.get('error', 'Unknown error').lower() if result else 'unknown error'
            print(f"{worker_str}Processing attempt failed for {url}. Error: {error_message}")
            is_proxy_related_error = any(term in error_message for term in [
                'proxy', 'timeout', 'connection', 'reset', 'refused', 'tunnel',
                'empty response', 'err_empty_response',
                'failed to load page',
                'net::err'
            ])

            if is_proxy_related_error and proxy_restart_count < max_proxy_restarts:
                proxy_restart_count += 1
                print(f"{worker_str}Proxy-related error detected. Restarting proxy (attempt {proxy_restart_count}/{max_proxy_restarts})...")

                new_proxy_info = proxy_manager.restart_proxy_for_worker(worker_id)

                if new_proxy_info:
                    print(f"{worker_str}Successfully restarted proxy. Retrying scrape...")
                    modified_entry['local_proxy'] = new_proxy_info['url']
                    proxy_info = new_proxy_info
                    time.sleep(3)
                    continue
                else:
                    print(f"{worker_str}FATAL - Failed to restart proxy for {url}. Aborting.")
                    result = {'status': 'error', 'error': 'Failed to restart proxy', 'url': url}
                    break

            else:
                 if is_proxy_related_error:
                      print(f"{worker_str}Max proxy restarts ({max_proxy_restarts}) reached for {url}. Not retrying.")
                 else:
                      print(f"{worker_str}Non-proxy related error encountered for {url}. Not retrying.")
                 result = {'status': 'error', 'error': last_error_message, 'url': url}
                 break

    with status_lock:
        if url in in_progress:
            in_progress.remove(url)
        if result and result.get('status') == 'success':
            shared_stats['processed'] += 1
            shared_stats['worker_status'][worker_id]['status'] = 'idle'
        elif result and result.get('status') == 'skipped':
             shared_stats['worker_status'][worker_id]['status'] = 'idle'
        else:
            shared_stats['errors'] += 1
            error_type = result.get('error', 'unknown error') if result else 'unknown'
            shared_stats['worker_status'][worker_id]['status'] = f'error ({error_type[:30]})'

    return result or {'status': 'error', 'error': f'Loop finished with no result. Last error: {last_error_message}', 'url': url}

def worker_function(worker_id, shared_stats, status_lock, work_queue, in_progress):
    """The main function executed by each worker process.

    Initializes a proxy, then continuously fetches entries from the work queue,
    processes them using `process_entry`, and handles errors until the queue is empty.

    Args:
        worker_id (int): The unique ID assigned to this worker process.
        shared_stats (multiprocessing.Manager.dict): Shared dictionary for statistics.
        status_lock (multiprocessing.Manager.Lock): Lock for accessing shared resources.
        work_queue (multiprocessing.Manager.Queue): Queue from which to fetch project entries.
        in_progress (multiprocessing.Manager.list): Shared list of URLs currently being processed.
    """
    worker_str = f"Worker {worker_id}: "
    print(f"{worker_str}Starting")

    with status_lock:
        shared_stats['worker_status'][worker_id] = {
            'status': 'initializing',
            'start_time': time.time(),
            'url': ''
        }

    initial_proxy_info = None
    max_proxy_init_attempts = 3
    for attempt in range(max_proxy_init_attempts):
        print(f"{worker_str}Initializing proxy (attempt {attempt+1}/{max_proxy_init_attempts})...")
        try:
            initial_proxy_info = proxy_manager.get_proxy_for_worker(worker_id)
            if initial_proxy_info:
                print(f"{worker_str}Successfully initialized proxy on {initial_proxy_info['url']}")
                with status_lock: shared_stats['workers_active'] += 1
                break
            else:
                print(f"{worker_str}Proxy initialization failed (attempt {attempt+1}), waiting before retry...")
                time.sleep(10)
        except Exception as e:
            print(f"{worker_str}Error during proxy initialization: {str(e)}")
            time.sleep(10)
    else:
        print(f"{worker_str}FATAL - Failed to initialize proxy after {max_proxy_init_attempts} attempts. Worker exiting.")
        with status_lock:
            shared_stats['errors'] += 1
            shared_stats['worker_status'][worker_id]['status'] = 'error (proxy init failed)'
        return

    while True:
        entry = None
        try:
            try:
                entry = work_queue.get(timeout=5)
                url = entry['data']['urls']['web']['project']
                entry_id = entry['data']['id']
            except queue.Empty:
                print(f"{worker_str}Work queue empty. Worker exiting.")
                break

            checkpoint, _ = load_checkpoint()
            with status_lock:
                url_in_progress = url in in_progress
                url_processed = url in checkpoint['processed_urls']

                if url_in_progress or url_processed:
                    status = "in progress by another worker" if url_in_progress else "already processed"
                    continue
                
                in_progress.append(url)

            delay = random.uniform(1, 3)

            process_entry(entry, worker_id, shared_stats, status_lock, work_queue, in_progress)

        except Exception as e:
            print(f"{worker_str}FATAL UNEXPECTED ERROR in main loop: {type(e).__name__} - {str(e)}")
            traceback.print_exc()
            if entry:
                 url = entry.get('data', {}).get('urls', {}).get('web', {}).get('project')
                 if url:
                      with status_lock:
                           if url in in_progress:
                                in_progress.remove(url)
                           shared_stats['errors'] += 1
                           shared_stats['worker_status'][worker_id]['status'] = 'error (unexpected)'

            time.sleep(5)
            continue

    with status_lock:
        if shared_stats['worker_status'][worker_id]['status'] != 'error (proxy init failed)':
             if shared_stats['workers_active'] > 0:
                 shared_stats['workers_active'] -= 1
        shared_stats['worker_status'][worker_id]['status'] = 'finished'

    print(f"{worker_str}Finished.")

def get_remaining_entries(all_entries, shared_stats, status_lock):
    """Filters a list of entries, removing those already processed based on checkpoint data.

    DEPRECATED: This function appears to be superseded by `get_entries_to_process`.
    It loads the checkpoint and filters based on processed URLs/IDs found there.

    Args:
        all_entries (list[dict]): The full list of entries loaded from the source.
        shared_stats (multiprocessing.Manager.dict): Shared statistics dictionary.
        status_lock (multiprocessing.Manager.Lock): Lock for accessing shared_stats.

    Returns:
        list[dict]: A list of entries that have not yet been processed.
    """
    checkpoint, results = load_checkpoint()
    processed_urls = set(checkpoint.get('processed_urls', []))
    processed_ids = set(item.get('id') for item in results if item.get('url') in processed_urls)

    remaining_entries = []
    processed_count_current = 0
    for entry in all_entries:
        try:
            url = entry['data']['urls']['web']['project']
            entry_id = entry['data']['id']
            if url not in processed_urls and entry_id not in processed_ids:
                remaining_entries.append(entry)
            else:
                 processed_count_current +=1
        except (KeyError, TypeError) as e:
             print(f"Warning: Skipping entry due to missing key/invalid structure: {e} - Entry ID: {entry.get('data',{}).get('id','N/A')}")

    with status_lock:
        shared_stats['total'] = len(remaining_entries)
        shared_stats['processed'] = processed_count_current

    print(f"Total entries loaded: {len(all_entries)}")
    print(f"Already processed (in checkpoint/results): {processed_count_current}")
    print(f"Remaining entries to process: {len(remaining_entries)}")

    return remaining_entries

def progress_reporter(shared_stats, status_lock, work_queue, in_progress):
    """Periodically prints progress statistics to the console.

    Runs in a separate thread and reports counts (processed, total, errors),
    rate, queue size, active workers, and workers currently processing tasks.
    Exits when no workers are active and the queue is empty.

    Args:
        shared_stats (multiprocessing.Manager.dict): Shared statistics dictionary.
        status_lock (multiprocessing.Manager.Lock): Lock for accessing shared_stats.
        work_queue (multiprocessing.Manager.Queue): The work queue (for size reporting).
        in_progress (multiprocessing.Manager.list): List of URLs being processed (for count).
    """
    start_time = time.time()
    last_processed_count = 0

    while True:
        with status_lock:
            active_workers = shared_stats.get('workers_active', 0)
            processed = shared_stats.get('processed', 0)
            total = shared_stats.get('total', 0)
            in_progress_count = len(in_progress)
            queue_size = work_queue.qsize() if work_queue else 0

        if active_workers == 0 and queue_size == 0 and in_progress_count == 0:
            print("\nProgress Reporter: No active workers, queue empty, and no tasks in progress. Exiting.")
            break

        time.sleep(PROGRESS_INTERVAL)

        with status_lock:
            processed = shared_stats.get('processed', 0)
            total = shared_stats.get('total', 0)
            errors = shared_stats.get('errors', 0)
            active_workers = shared_stats.get('workers_active', 0)
            in_progress_count = len(in_progress)

        elapsed_time = time.time() - start_time
        processed_since_last = processed - last_processed_count
        rate = (processed_since_last / PROGRESS_INTERVAL) * 60 if elapsed_time > 0 and processed_since_last > 0 else 0
        last_processed_count = processed

        percentage = (processed / total) * 100 if total > 0 else 0
        time_str = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

        print(f"\n--- PROGRESS [{datetime.now().strftime('%H:%M:%S')}|Elapsed:{time_str}] ---")
        print(f"Processed: {processed}/{total} ({percentage:.1f}%) | Errors: {errors}")
        print(f"Rate: {rate:.1f}/min | Queue: {queue_size} | In Progress: {in_progress_count} | Active Workers: {active_workers}")

        processing_workers = []
        for worker_id, status_info in shared_stats['worker_status'].items():
            if isinstance(status_info, dict) and status_info.get('status') == 'processing':
                 url = status_info.get('url', 'N/A')
                 start = status_info.get('start_time', 0)
                 elapsed_worker = time.time() - start if start else 0
                 processing_workers.append(f"W{worker_id}({elapsed_worker:.0f}s)")
        if processing_workers:
             print(f"Processing: {', '.join(processing_workers)}")
        else:
             idle_workers = sum(1 for info in shared_stats['worker_status'].values() if isinstance(info, dict) and info.get('status') == 'idle')
             error_workers = sum(1 for info in shared_stats['worker_status'].values() if isinstance(info, dict) and 'error' in info.get('status', ''))

        if active_workers == 0 and queue_size == 0 and in_progress_count == 0:
             print("\nProgress Reporter: Conditions met for exit after reporting.")
             break

def evaluate_canceled_project(entry):
    """Evaluates if a 'canceled' project meets criteria to be treated as 'failed'.

    A canceled project is reclassified as 'failed' if:
    1. It did not reach its funding goal (pledged < goal).
    2. Its state changed (canceled) after at least 40% of its planned duration had passed.

    Args:
        entry (dict): The Kickstarter project entry dictionary.

    Returns:
        bool: True if the canceled project should be reclassified as failed, False otherwise.
    """
    try:
        data_section = entry.get('data', {})
        created_at = int(data_section.get('created_at', 0))
        deadline = int(data_section.get('deadline', 0))
        state_changed_at = int(data_section.get('state_changed_at', 0))

        if not created_at or not deadline or created_at >= deadline:
             return False 
        if state_changed_at == 0:
            state_changed_at = int(data_section.get('last_update_published_at', 0))
            if state_changed_at == 0:
                 return False 

        total_duration = deadline - created_at
        threshold_time = created_at + (0.4 * total_duration)

        if state_changed_at <= threshold_time:
            return False

        goal = float(data_section.get('goal', 0.0))
        usd_exchange_rate = float(data_section.get('usd_exchange_rate', 1.0))
        goal_usd = max(1.0, goal * usd_exchange_rate) 

        pledged = float(data_section.get('converted_pledged_amount', 0.0))

        if pledged >= goal_usd:
            return False

        return True

    except (KeyError, ValueError, TypeError) as e:
        print(f"Error evaluating canceled project {entry.get('data',{}).get('id','N/A')}: {str(e)}")
        return False

def get_entries_to_process(total_needed, seed=None):
    """Loads entries, filters them, and selects a random subset for processing.

    1. Loads the checkpoint to determine already processed entries.
    2. Calculates how many new entries are needed to reach `total_needed`.
    3. Loads all entries from the JSON source.
    4. Filters entries:
        - Keeps 'successful' and 'failed' states.
        - Evaluates 'canceled' entries using `evaluate_canceled_project` and
          includes them (reclassified as 'failed') if they meet the criteria.
        - Excludes entries already processed (based on checkpoint).
    5. Randomly shuffles the eligible entries (if a seed is provided).
    6. Selects the required number of new entries.

    Args:
        total_needed (int): The target total number of processed entries.
        seed (int, optional): A random seed for shuffling. Defaults to None.

    Returns:
        list[dict]: A list of selected entry dictionaries to be processed.
    """
    if seed is not None:
        random.seed(seed)

    checkpoint, results = load_checkpoint()
    processed_urls = set(checkpoint.get('processed_urls', []))
    processed_ids = set(item.get('id') for item in results if item.get('url') in processed_urls)

    already_processed_count = len(processed_urls) 
    entries_needed = max(0, total_needed - already_processed_count)

    if entries_needed == 0:
        print(f"Target of {total_needed} entries already processed. No new entries needed.")
        return []

    print(f"Target: {total_needed} entries | Already processed: {already_processed_count} | Need {entries_needed} more.")

    all_entries = load_entries_from_json(LOCAL_JSON_FILE)
    if not all_entries:
        print("ERROR: No entries loaded from JSON file. Cannot proceed.")
        return [] 

    print(f"Loaded {len(all_entries)} entries from {LOCAL_JSON_FILE}")

    eligible_entries = []
    evaluated_canceled_count = 0
    included_canceled_count = 0

    for entry in all_entries:
        try:
            entry_data = entry['data']
            entry_id = entry_data['id']
            entry_url = entry_data['urls']['web']['project']
            entry_state = entry_data.get('state', '')

            if entry_url in processed_urls or entry_id in processed_ids:
                continue

            if entry_state in ['successful', 'failed']:
                eligible_entries.append(entry)

            elif entry_state == 'canceled':
                evaluated_canceled_count += 1
                if evaluate_canceled_project(entry):
                    entry['data']['state'] = 'failed' 
                    eligible_entries.append(entry)
                    included_canceled_count += 1

        except (KeyError, TypeError) as e:
            print(f"Warning: Skipping entry due to missing key/invalid structure: {e} - Entry ID: {entry.get('data',{}).get('id','N/A')}")
            continue

    print(f"Evaluated {evaluated_canceled_count} 'canceled' projects, included {included_canceled_count} (reclassified as 'failed').")
    print(f"Found {len(eligible_entries)} eligible entries (successful, failed, or reclassified canceled) not yet processed.")

    if len(eligible_entries) < entries_needed:
        print(f"Warning: Only {len(eligible_entries)} eligible entries available, less than the {entries_needed} needed to reach target.")
        selected_entries = eligible_entries 
    else:
        random.shuffle(eligible_entries)
        selected_entries = eligible_entries[:entries_needed]

    print(f"Selected {len(selected_entries)} new entries for processing.")
    return selected_entries

def main():
    """Main execution function.

    Sets up multiprocessing components (manager, queue, lock, shared dict),
    loads and selects entries to process, starts worker processes and the
    progress reporter thread, waits for workers to complete, and handles cleanup.
    """
    atexit.register(proxy_manager.stop_all_proxies)
    data_source_path = LOCAL_JSON_FILE
    if not os.path.exists(data_source_path):
        print(f"Error: Data source file not found at {data_source_path}")
        print("Please ensure the LOCAL_JSON_FILE path in single_processor.py is correct.")
        return
    print(f"Using data source: {data_source_path}")

    manager = multiprocessing.Manager()
    shared_stats = manager.dict({
        'processed': 0,
        'total': 0,
        'errors': 0,
        'workers_active': 0,
        'worker_status': manager.dict(),
    })
    status_lock = manager.Lock()
    work_queue = manager.Queue()
    in_progress = manager.list()

    random.seed(RANDOM_SEED)

    print("Loading entries and determining tasks...")
    entries_to_process = get_entries_to_process(TOTAL_ENTRIES, seed=RANDOM_SEED)

    if not entries_to_process:
        print("No new entries selected for processing. Exiting.")
        return

    for entry in entries_to_process:
        work_queue.put(entry)

    print(f"Added {len(entries_to_process)} entries to the work queue.")

    worker_count = min(NUM_WORKERS, len(entries_to_process))
    print(f"Starting {worker_count} workers...")
    proxy_manager.assign_proxy_hosts_to_workers(worker_count)

    processes = []
    for i in range(worker_count):
        p = multiprocessing.Process(
            target=worker_function,
            args=(i, shared_stats, status_lock, work_queue, in_progress),
            daemon=True 
        )
        processes.append(p)
        p.start()
        time.sleep(0.2)

    progress_thread = threading.Thread(
        target=progress_reporter,
        args=(shared_stats, status_lock, work_queue, in_progress),
        daemon=True,
        name="ProgressReporter"
    )
    progress_thread.start()

    print("Main process waiting for workers to complete...")
    try:
        for p in processes:
            p.join() 
        print("All worker processes have joined.")

        progress_thread.join(timeout=PROGRESS_INTERVAL + 5)
        if progress_thread.is_alive():
             print("Warning: Progress reporter thread did not exit cleanly.")

    except KeyboardInterrupt:
         print("\nMain process received KeyboardInterrupt. Exiting.")

    print("\nAll workers completed or terminated.")

def signal_handler(sig, frame):
    """Handles termination signals (SIGINT, SIGTERM) for graceful shutdown."""
    print(f"\nReceived signal {sig}, initiating shutdown...")
    proxy_manager.stop_all_proxies()
    print("Proxy manager cleanup requested.")
    sys.exit(0)


if __name__ == "__main__":
    multiprocessing.freeze_support() 
    signal.signal(signal.SIGINT, signal_handler) 
    signal.signal(signal.SIGTERM, signal_handler) 

    if os.name == 'nt':
        try:
            signal.signal(signal.CTRL_CLOSE_EVENT, signal_handler)
        except (AttributeError, ValueError):
            print("Warning: Could not register CTRL_CLOSE_EVENT handler")
    if "-n" not in sys.argv:
        sys.argv.append("-n")

    try:
        main()
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt caught in __main__. Exiting.")
    except Exception as e:
        print(f"\nFATAL UNHANDLED EXCEPTION in main execution: {type(e).__name__} - {str(e)}")
        traceback.print_exc()
    finally:
        print("Main execution finished or terminated.")
