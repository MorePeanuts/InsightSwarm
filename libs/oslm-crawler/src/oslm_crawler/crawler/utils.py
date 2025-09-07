import queue
import threading
from typing import Generator
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from webdriver_manager.chrome import ChromeDriverManager
from loguru import logger

def init_driver() -> WebDriver:
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    return driver


class WebDriverPool:
    
    def __init__(self, size: int = 1, options: ChromeOptions | None = None):
        if options is None:
            options = ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
        self.options = options
        self.size = size
        self._pool = queue.Queue(maxsize=size)
        self._lock = threading.Lock()
        self._initialize_pool()
        
    def _initialize_pool(self):
        for _ in range(self.size):
            driver = self._create_driver()
            self._pool.put(driver)
            
            
    def _create_driver(self) -> WebDriver:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=self.options)
        return driver

    def _is_driver_healthy(self, driver: WebDriver) -> bool:
        try:
            driver.current_url
            return True
        except Exception:
            logger.exception("Web driver healthy check failed.")
            return False
        
    @contextmanager
    def get_driver(self) -> Generator[WebDriver, None, None]:
        driver = None
        try:
            driver: WebDriver = self._pool.get()
            if not self._is_driver_healthy(driver):
                try:
                    driver.quit()
                except Exception:
                    logger.exception("Web driver quit failed when quit unhealthy driver.")
                    pass
                driver = self._create_driver()
            
            yield driver
        except Exception:
            logger.exception("Web driver get failed.")
            raise
        finally:
            if driver is not None:
                self._pool.put(driver)
                
    def cleanup(self):
        with self._lock:
            tmp_drivers = []
            try:
                while not self._pool.empty():
                    driver = self._pool.get_nowait()
                    tmp_drivers.append(driver)
            except queue.Empty:
                logger.exception("Empty driver queue exception.")
                pass
            
            for driver in tmp_drivers:
                try:
                    driver.quit()
                except Exception:
                    logger.exception("Web driver quit failed when cleanup pool.")
                    pass
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False
    
    def __del__(self):
        self.cleanup()


def str2int(s: str) -> int:
    """
    Examples:
    -----
    >>> str2int("295,137")
    295137
    >>> str2int("1.7k")
    1700
    >>> str2int("3.1m")
    3100000
    >>> str2int("38k")
    38000
    >>> str2int("")
    0
    >>> str2int(None)
    0
    >>> str2int("-")
    0
    >>> str2int(1234)
    1234
    """
    if isinstance(s, int):
        return s
    if s is None or s == "" or s == "-":
        return 0
    if "," in s:
        s = s.replace(",", "")
    try:
        if "k" in s or "K" in s:
            s = s.replace("k", "").replace("K", "")
            return int(float(s) * 1_000)
        elif "m" in s or "M" in s:
            s = s.replace("m", "").replace("M", "")
            return int(float(s) * 1_000_000)
        elif "b" in s or "B" in s:
            s = s.replace("b", "").replace("B", "")
            return int(float(s) * 1_000_000_000)
        else:
            return int(s)
    except ValueError as e:
        raise Exception("str2int error") from e