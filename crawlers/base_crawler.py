from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.common.exceptions import NoSuchWindowException, WebDriverException

from utils import Logger, Progress, LinkExtractor, Cookies
from utils.colors import *
from utils.utils import login, is_logged_in, ordinal
from pipeline import Pipeline

import json
import sys
import time
import logging
from os.path import join
from urllib.parse import urlparse
from traceback import format_exc
from scipy.stats import weibull_min
from typing import Any, Sequence

LOGGER.setLevel(logging.CRITICAL)


class BaseCrawler:
    """
    Base class for crawlers
    """

    CRITICAL_EXCEPTIONS = [
        KeyboardInterrupt,
        NotImplementedError,
        NoSuchWindowException,
        WebDriverException,
    ]

    def __init__(
        self,
        chromedriver_path: str,
        navigate_link_extractor: LinkExtractor,
        parse_link_extractor: LinkExtractor,
        crawler_dir: str,
        data_pipeline: Pipeline,
        user: str,
        secrets_file: str,
        cookies_save_dir: str,
        headless: bool = True,
        sleep_weibull_lambda: float = 10.0,
        max_loading_wait: float = 90,
        max_error_trials: int = 5,
        name: str = "Crawler",
    ):
        self.logger = Logger(name)
        self.logger.info("Initializing...")
        self.navigate_link_extractor = navigate_link_extractor
        self.parse_link_extractor = parse_link_extractor
        self.set_crawler_dir(crawler_dir=crawler_dir, data_pipeline=data_pipeline)
        self.progress = Progress(dir=join(crawler_dir, "progress"))
        self.progress.load()
        self.user = user
        self.secret_file = secrets_file
        self.cookies = Cookies(user=user, save_dir=cookies_save_dir)

        self.headless = headless
        self.sleep_weibull_lambda = sleep_weibull_lambda
        self.max_loading_wait = max_loading_wait
        self.max_error_trials = max_error_trials

        self.chromedriver_path = chromedriver_path
        self.driver_service = Service(chromedriver_path)
        self.driver_options = webdriver.ChromeOptions()

        # Options
        ## Disable image loading
        self.driver_options.add_argument("--blink-settings=imagesEnabled=false")
        ## Disable notifications
        self.driver_options.add_argument("--disable-notifications")
        ## Enable headless, or else detach mode
        if headless:
            self.driver_options.add_argument("--headless")
        else:
            self.driver_options.add_experimental_option("detach", True)

    def on_start(self):
        # raise NotImplementedError("Crawler's on_start method is not implemented")
        pass

    def on_exit(self):
        # raise NotImplementedError("Crawler's on_exit method is not implemented")
        pass

    def on_parse_error(self):
        # raise NotImplementedError("Crawler's on_parse_error method is not implemented")
        pass

    def parse(self) -> dict[str, Any | Sequence[Any]] | list[dict[str, Any]]:
        raise NotImplementedError("Crawler's parse method is not implemented")

    def new_tab(self, url: str):
        self.chrome.switch_to.new_window("tab")
        self.chrome.get(url)
        self.logger.info(f"Opened new tab to {grey(url)}")

    def close_all_new_tabs(self):
        for handle in self.chrome.window_handles:
            if handle == self.main_tab:
                continue
            self.chrome.switch_to.window(handle)
            self.chrome.close()
        self.chrome.switch_to.window(self.main_tab)

    def set_crawler_dir(self, crawler_dir: str, data_pipeline: Pipeline):
        for step in data_pipeline.steps:
            step.set_path_format(crawler_dir=crawler_dir)
        self.crawler_dir = crawler_dir
        self.data_pipeline = data_pipeline

    def sleep(self):
        sleep_second = weibull_min.rvs(10, loc=0, scale=self.sleep_weibull_lambda)
        time.sleep(sleep_second)

    def wait_DOM(self):
        self.chrome.implicitly_wait(self.max_loading_wait)

    def start_driver(self):
        self.chrome = webdriver.Chrome(
            service=self.driver_service, options=self.driver_options
        )
        self.main_tab = self.chrome.current_window_handle
        self.logger.info(f"Driver started")

    def save_cookies(self):
        self.cookies.save(self.chrome.get_cookies())

    def load_cookies(self):
        self.chrome.get("https://www.facebook.com")
        for cookie in self.cookies.load():
            self.chrome.add_cookie(cookie)

    def save_progress(self):
        self.progress.save()

    def load_progress(self):
        self.progress.load()

    def ensure_logged_in(self):
        self.logger.info("Ensuring user logging in")
        if self.cookies.exists():
            self.logger.info("Found user's credentials cached as cookies")
            self.load_cookies()
            return

        url = urlparse(self.chrome.current_url)
        domain_name = (
            ".".join(url.hostname.split(".")[-2:]) if url.hostname is not None else None
        )
        if domain_name != "https://facebook.com":
            self.chrome.get("https://www.facebook.com")
        if is_logged_in(self.chrome):
            self.logger.info("User is already logged in inside browser")
            return

        self.logger.info(
            f"No existing credentials found, manuallly signing in for {self.user}"
        )
        with open(self.secret_file, "r") as f:
            user_info = json.load(f)[self.user]
        login(
            self.chrome, username=user_info["username"], password=user_info["password"]
        )

    def extract_urls_from_current_page(self):
        url = self.chrome.current_url
        html = self.chrome.page_source

        new_nav_urls = self.navigate_link_extractor.extract(html)
        new_parse_urls = self.parse_link_extractor.extract(html)
        try:
            new_nav_urls.remove(url)
        except:
            pass
        try:
            new_parse_urls.remove(url)
        except:
            pass

        self.logger.info(f"Enqueuing {len(new_nav_urls)} new navigation URLs")
        self.progress.selectively_enqueue_list(new_nav_urls, ignore="history")

        self.logger.info(
            f"Selectively enqueuing {len(new_parse_urls)} new parsing URLs"
        )
        self.progress.selectively_enqueue_list(new_parse_urls)

    def start(self, start_url: str):
        self.start_driver()
        self.on_start()

        self.ensure_logged_in()
        self.save_cookies()
        self.logger.info("Saved/Refreshed cookies")

        if len(self.progress.queue) == 0 or self.progress.queue[0] != start_url:
            self.progress.selectively_enqueue(start_url, side="left", ignore="history")
        err_trial = 0

        while (
            self.progress.count_remaining() > 0 and err_trial <= self.max_error_trials
        ):
            try:
                exc_type = None
                url = self.progress.next_url()

                # If URL is for navigation
                if self.navigate_link_extractor.match(url) or url == start_url:
                    self._handle_navigation_url(url)
                # If URL is for parsing
                if self.parse_link_extractor.match(url):
                    self._handle_parse_url(url)

                self.progress.add_history(url)
                err_trial = 0
                self.sleep()
            except:
                err_trial += 1
                # Logging out error
                exc_type, value, tb = sys.exc_info()
                self.logger.error(
                    f"Restore {grey(url)} to queue due to error: \n{red(exc_type.__name__)}: {value}\n{format_exc()}"
                )
                # If this url hasn't been crawled successfully
                if not self.progress.propagated(url):
                    # Re-append URL to queue
                    self.progress.enqueue(url, "left")

                self.on_parse_error()
                self.close_all_new_tabs()
                # If error due to no abstract method implementation, stop retrying
                if exc_type in BaseCrawler.CRITICAL_EXCEPTIONS:
                    break
                if err_trial <= err_trial:
                    self.logger.warning(
                        f"Attempting {bold(ordinal(err_trial))} retrial..."
                    )
                self.sleep()

        if exc_type is not None:
            self.logger.error(f"Closing driver due to an error occured...")
        elif err_trial > self.max_error_trials:
            self.logger.error(
                "Maximum number of trials upon errors exceeded, exitting..."
            )
        elif self.progress.count_remaining() == 0:
            self.logger.info("Closing driver due to no URL left in queue...")
        self.save_progress()
        self.on_exit()
        self.chrome.quit()

    def _handle_navigation_url(self, url: str):
        self.logger.info(f"Matched as URL for {bold('navigation')}: {grey(url)}")
        self.chrome.get(url)
        self.wait_DOM()

        self.extract_urls_from_current_page()

    def _handle_parse_url(self, url: str):
        self.logger.info(f"Matched as URL for {bold('parsing')}: {grey(url)}")
        self.new_tab(url)
        self.wait_DOM()

        data = self.parse()
        self.data_pipeline(data)

        self.close_all_new_tabs()