from selenium.webdriver.remote.webelement import WebElement
import re
from datetime import datetime


def parse_post_date(raw_data: str):
    raw_data = raw_data.lower()
    raw = re.search(
        r"\d{1,2} tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{1,2}",
        raw_data,
    ).group(0)
    result = datetime.strptime(raw, "%d tháng %m, %Y lúc %H:%M")
    return result


def parse_text_from_element(text_element: WebElement):
    text = text_element.get_attribute("innerHTML")
    text = re.sub(r"(<img[^>]*alt=\"([^\"]+)\")[^>]*>", r"\2", text)
    text = re.sub(r"<a[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>", r"href(\2, \1)", text)
    text = re.sub(r"(?<=</div>)()(?=<div)", r"\n", text)
    text = re.sub(r"<.*?>", "", text)
    return text
