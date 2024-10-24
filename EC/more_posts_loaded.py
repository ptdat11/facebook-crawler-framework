from typing import Any
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver import Chrome


class more_posts_loaded:
    def __init__(self, posts_locator: tuple) -> None:
        self.posts_locator = posts_locator
        self.current_post_count = 0

    def __call__(self, driver: Chrome):
        by, xpath = self.posts_locator
        current_post_count = len(driver.find_elements(by, xpath))

        prev_post_count = self.current_post_count
        self.current_post_count = current_post_count

        return current_post_count > prev_post_count
