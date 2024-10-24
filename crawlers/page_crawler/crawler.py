from typing import Any, Sequence
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Keys, ActionChains

import re
import bs4
from ..base_crawler import BaseCrawler
from EC import more_posts_loaded
from utils.parsing import parse_post_date, parse_text_from_element
from utils.utils import to_bs4

from html import unescape
from datetime import datetime
from typing import Literal
from psutil import virtual_memory
from tqdm import tqdm


class Crawler(BaseCrawler):
    posts_xpath = "(//div[@class='x9f619 x1n2onr6 x1ja2u2z xeuugli xs83m0k xjl7jj x1xmf6yo x1emribx x1e56ztr x1i64zmx x19h7ccj xu9j1y6 x7ep2pv']/div)[last()]/div/div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']"
    content_on_hover_xpath = (
        "(//div[@class='x78zum5 xdt5ytf x1n2onr6 xat3117 xxzkxad']/div)[2]/div"
    )
    hashtag_regex = re.compile(r"#[^\s,]+")
    emoji_src_map = {
        "An-HX414PnqCVzyEq9OFFdayyrdj8c3jnyPbPcierija6hpzsUvw-1VPQ260B2M9EbxgmP7pYlNQSjYAXF782_vnvvpDLxvJQD74bwdWEJ0DhcErkDga6gazZZUYm_Q.png": "like",
        "An8VnwvdkGMXIQcr4C62IqyP-g1O5--yQu9PnL-k4yvIbj8yTSE32ea4ORp0OwFNGEWJbb86MHBaLY-SMvUKdUYJnNFcexEoUGoVzcVd50SaAIzBE-K5dxR8Y-MJn5E.png": "love",
        "An-POmkU-_NNTTsdRMlBuMNo0AY4ErdT38vLDNtGKtUSrILEfybR2XqG2yRrfGfN1vBl3SAsfomCLcWikp72R2ay__g5C5Ufwb-77V2qflOKGqve2111p7Pu_qihMMCw.png": "angry",
        "An95QHaxAbMTp2SyUXLpDATL4RVXaXWyMMPZdhhNbQvXSEtO4mBobyhl440IsX6aUdwySIdlo5h4V7oqQ3FNgrsS1ZCe5rj7-534rtBlVLAm3GMjBK9wsB53peUgOw.png": "care",
        "An-r5ENfro_aq4TtchBwMAVpq461_uMMZX8CbykXeZm3K5tLEtYF2nA1Pcw8d0sbbq0OlIGksDIXALp3ar6dWf5LBjKs9OFlVqQY0wT42aI9jmUG62LKClEYB7Msj7Q.png": "wow",
        "An8jKAygX0kuKnUS351UNmsULZ5k4-fMTFmFHmO7SrQJO1CWNfvoTzEEAr5ZjSoZJRjncZcWMCU1B4of5Vw7bMygV5NmjoeSdthAyQVsakIDduXmYDseOeVRf40MOA.png": "haha",
        "An855a_dxeehKWf2PSOqZw5jG_X5jD0RtPu4XCOJEiUkOgEjN08FocslKz_Ex-1X4l2nyxwET8fM7vQtp4UWea1ndn808NC5OXHaPll4vMdgaoE8ttu-hOlUSetdVjU.png": "sad",
    }

    class PostCollectCriterion:
        def __init__(
            self,
            criterion: Literal["elapsed_minutes", "n_posts", "post_time"],
            threshold: float | int | datetime,
        ) -> None:
            self.criterion = criterion
            self.threshold = threshold
            self.reset()

        def reset(self):
            if self.criterion == "elapsed_minutes":
                self.start = datetime.now()
                self.progress = 0.0
            elif self.criterion == "n_posts":
                self.progress = 0
            elif self.criterion == "post_time":
                self.progress = datetime.now()

        def update_progress(self, driver: Chrome):
            if self.criterion == "elapsed_minutes":
                self.progress = (datetime.now() - self.start).total_seconds() / 60
            elif self.criterion == "n_posts":
                self.progress = len(driver.find_elements(By.XPATH, Crawler.posts_xpath))
            elif self.criterion == "post_time":
                datetime_div = driver.find_element(
                    By.XPATH, Crawler.content_on_hover_xpath
                )
                last_post_datetime_a = driver.find_element(
                    By.XPATH,
                    f"(({Crawler.posts_xpath})[last()]//h2/../../../../div)[2]//a",
                )
                ActionChains(driver).move_to_element(last_post_datetime_a).perform()
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "(//div[@class='x78zum5 xdt5ytf x1n2onr6 xat3117 xxzkxad']/div)[2]/div/div",
                        )
                    )
                )
                soup = to_bs4(datetime_div)
                raw_datetime = soup.text
                self.progress = parse_post_date(raw_datetime)

        def condition_met(self):
            if self.criterion == "elapsed_minutes":
                return self.progress >= self.threshold
            elif self.criterion == "n_posts":
                return self.progress >= self.threshold
            elif self.criterion == "post_time":
                return self.progress <= self.threshold

    def __init__(
        self,
        page_id: str,
        post_collect_threshold: float | int | datetime,
        post_collect_criterion: Literal[
            "elapsed_minutes", "n_posts", "post_time"
        ] = "n_posts",
        max_ram_percentage: float = 0.8,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs, name="Page Crawler")

        self.post_collect_criteria = Crawler.PostCollectCriterion(
            criterion=post_collect_criterion,
            threshold=post_collect_threshold,
        )
        self.max_ram_percentage = max_ram_percentage
        self.page_id = page_id
        self.set_pipeline_path_format(page_id=page_id)

    def on_parse_error(self):
        self.post_collect_criteria.reset()

    def parse(self) -> dict[str, Any | Sequence[Any]]:
        with tqdm(
            total=round(virtual_memory().total / 1024**3, ndigits=2),
            desc="RAM Usage (GB)",
        ) as bar:
            # Scroll though page's feed
            while (
                ram_usage := virtual_memory()
            ).percent / 100 < self.max_ram_percentage and not (
                met := self.post_collect_criteria.condition_met()
            ):
                bar.n = round(ram_usage.used / 1024**3, ndigits=2)
                bar.refresh()

                self.chrome.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight)"
                )
                WebDriverWait(self.chrome, self.max_loading_wait).until(
                    more_posts_loaded(posts_locator=(By.XPATH, Crawler.posts_xpath))
                )
                self.post_collect_criteria.update_progress(self.chrome)
                bar.set_postfix_str(f"# Loaded posts: {len(self.get_loaded_posts())}")
                self.sleep()

        if met:
            self.logger.info(
                f"Post collect stopping criteria has met with threshold of {self.post_collect_criteria.threshold}"
            )

        to_be_removed = self.chrome.find_element(
            By.XPATH,
            "(//div[@class='x9f619 x1n2onr6 x1ja2u2z x78zum5 xdt5ytf xeuugli x1r8uery x1iyjqo2 xs83m0k x1swvt13 x1pi30zi xqdwrps x16i7wwg x1y5dvz6'])[3]",
        )
        self.chrome.execute_script("arguments[0].remove();", to_be_removed)

        to_be_removed = self.chrome.find_element(By.XPATH, "//div[@role='banner']")
        self.chrome.execute_script("arguments[0].remove();", to_be_removed)

        post_divs = self.get_loaded_posts()
        self.logger.info(f"Located {len(post_divs)} posts")
        items = []
        for i, post_div in tqdm(
            enumerate(post_divs, start=1), total=len(post_divs), desc="Parsing posts"
        ):
            ActionChains(self.chrome).move_to_element(post_div).pause(1).perform()
            post = self.parse_post(i, post_div)
            items.append(post)

        return items

    def get_loaded_posts(self):
        return self.chrome.find_elements(By.XPATH, Crawler.posts_xpath)

    def parse_post(self, i: int, post_div: WebElement):
        hover_content_div = self.chrome.find_element(
            By.XPATH, Crawler.content_on_hover_xpath
        )

        post_content_divs = self.chrome.find_elements(
            By.XPATH,
            f"(({Crawler.posts_xpath})[{i}]/descendant::div[@class='html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd'])[2]/div",
        )

        # Profile
        profile_div = post_content_divs[1].find_element(
            By.XPATH,
            f"({Crawler.posts_xpath})[{i}]/descendant::div[@data-ad-rendering-role='profile_name']",
        )
        owner_loc_anchors = profile_div.find_elements(
            By.XPATH,
            f"(({Crawler.posts_xpath})[{i}]//h2/../../../../div//a",
        )

        # Content
        content_div = post_content_divs[2]
        num_content_modalities = len(content_div.find_elements(By.XPATH, "div"))
        text_content_div = content_div.find_elements(
            By.XPATH, "./descendant::div[@data-ad-comet-preview='message']"
        )
        text_content_div = text_content_div[0] if len(text_content_div) > 0 else None
        if (
            num_content_modalities == 2
            and text_content_div is not None
            or num_content_modalities == 1
            and text_content_div is None
        ):
            visual_content_div = post_content_divs[2].find_element(
                By.XPATH, "(./div)[last()]"
            )
        else:
            visual_content_div = None

        # User interaction
        interaction_div = post_content_divs[3].find_element(
            By.XPATH, "./descendant::div[@class='x1n2onr6']/div"
        )
        reaction_div = interaction_div.find_element(By.XPATH, "./div/div")
        cmt_share_div = interaction_div.find_element(By.XPATH, "(./div)[last()]")

        num_comments, num_shares = 0, 0
        if len(to_bs4(cmt_share_div).find_all("div", {"role": "button"})) > 0:
            for btn in cmt_share_div.find_elements(
                By.XPATH, "./descendant::div[@role='button']"
            ):
                # ActionChains(self.chrome).move_to_element(btn).perform()
                # WebDriverWait(self.chrome, 10).until(
                #     EC.presence_of_element_located(
                #         (
                #             By.XPATH,
                #             f"{Crawler.content_on_hover_xpath}/descendant::div[contains(@class, '__fb-light-mode')]/descendant::span[@dir='auto']",
                #         )
                #     )
                # )
                # _soup = to_bs4(hover_content_div).find("span", {"dir": "auto"})

                # self.logger.info(_soup)
                # users = _soup.find_all("span")
                # has_others = (
                #     re.match(r"^\d+ (bình luận|lượt chia sẻ)$", users[-1].text)
                #     if len(users) > 0
                #     else False
                # )
                # count = len(users) + int(
                #     re.search(r"\d+", users[-1].text).group(0) if has_others else 0
                # )
                # btn_text = re.sub(r"\d+", "", btn.text).strip()
                btn_text_match = re.search(r"^(\d+) (.+)$", btn.text)
                count, btn_text = btn_text_match.group(1), btn_text_match.group(2)
                if btn_text == "bình luận":
                    num_comments = count
                elif btn_text == "lượt chia sẻ":
                    num_shares = count

        # Ensure date element appears
        post_datetime_a = profile_div.find_element(By.XPATH, "(../../../div)[2]//a")
        ActionChains(self.chrome).move_to_element(post_datetime_a).pause(0.3).perform()
        WebDriverWait(self.chrome, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    f"{Crawler.content_on_hover_xpath}/descendant::div[contains(@class, '__fb-light-mode')]",
                )
            )
        )
        datetime_soup = to_bs4(hover_content_div)
        raw_datetime = datetime_soup.text

        # Ensure post's text content showing full version
        if (
            to_bs4(content_div).find("div", attrs={"role": "button"}, string="Xem thêm")
            is not None
        ):
            show_more_btn = content_div.find_element(
                By.XPATH, "./descendant::div[@role='button' and text()='Xem thêm']"
            )
            ActionChains(self.chrome, 10).move_to_element(show_more_btn).click(
                show_more_btn
            ).pause(0.5).move_to_element(post_div).perform()

        # Gather reaction information
        ActionChains(self.chrome).click(reaction_div).pause(2).perform()
        # WebDriverWait(self.chrome, 10).until(
        #     EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog']"))
        # )
        reaction_modal = self.chrome.find_element(By.XPATH, "//div[@role='dialog']")
        reaction_counts = reaction_modal.find_elements(
            By.XPATH,
            "./descendant::div[@class='x1swvt13 x1pi30zi']/descendant::div[@class='x6ikm8r x10wlt62 xlshs6z']/div",
        )
        modal_close = reaction_modal.find_element(
            By.XPATH, "./descendant::div[@class='x1d52u69 xktsk01']/div"
        )
        reaction = ""
        for _reaction in reaction_counts:
            if (
                text := to_bs4(_reaction.find_element(By.XPATH, ".//span")).text
            ) == "Tất cả":
                continue
            icon_src = _reaction.find_element(By.XPATH, ".//img").get_attribute("src")
            icon_src = re.search(r"/t6/([^\.]+\.png)\?", icon_src).group(1)
            count = text
            reaction += f"{Crawler.emoji_src_map[icon_src]} ({count});"
        reaction = reaction.strip(";")
        modal_close.click()

        post_link = (
            re.search(
                rf"^https://www\.facebook\.com/{self.page_id}/[^/]+/[^\?\s]+\?",
                post_datetime_a.get_attribute("href"),
            )
            .group(0)
            .strip("/?")
        )
        post_date = parse_post_date(raw_datetime)
        owner = owner_loc_anchors[2].text
        location = (
            owner_loc_anchors[3].text
            if owner_loc_anchors[3]
            .find_element(By.XPATH, "span")
            .get_attribute("class")
            == "xt0psk2"
            else None
        )
        content = (
            parse_text_from_element(text_content_div)
            if text_content_div is not None
            else ""
        )
        hashtag = " ".join(Crawler.hashtag_regex.findall(content))
        content = Crawler.hashtag_regex.sub("", content)
        content = unescape(re.sub(r"href\(, [^\)]+\)", "", content).strip())
        visual_soup = (
            to_bs4(visual_content_div) if visual_content_div is not None else None
        )
        is_post_image = (
            visual_soup.find("img") is not None
            and "data-visualcompletion" not in visual_soup.find("img").parent.attrs
            if visual_soup is not None
            else False
        )
        is_post_video = (
            visual_soup.find("div", {"role": "presentation"}) is not None
            if visual_soup is not None
            else False
        )

        return {
            "Post_link": post_link,
            "Owner": owner,
            "Location": location,
            "Post_date": post_date,
            "Content": content,
            "Hashtag": hashtag,
            "Is_Post_Image": is_post_image,
            "Is_Post_Video": is_post_video,
            "Reaction": reaction,
            "Num_comments": num_comments,
            "Num_share": num_shares,
            "Crawl_time": datetime.now(),
        }

    def start(self):
        super().start(start_url=f"https://www.facebook.com/{self.page_id}")
