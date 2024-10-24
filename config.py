from utils import LinkExtractor
from pipeline import Pipeline, SaveAsCSV, SaveAsExcel, HandleHrefs
from datetime import datetime

PIPELINE = Pipeline(
    HandleHrefs(action="keep_content"),
    SaveAsCSV(dst_dir="{crawler_dir}/{page_id}"),
    # SaveAsExcel(dst_dir="{crawler_dir}/{page_id}", sheet_name="Post"),
)

NAVIGATE_LINK_EXTRACTOR = LinkExtractor(allow_regex=r"", deny_regex=r".*")

PARSE_LINK_EXTRACTOR = LinkExtractor(
    allow_regex=r"https://www\.facebook\.com/[^/\s\?]+$", deny_regex=r""
)

CRAWLER_ARGUMENTS = {
    "page_crawler": dict(
        page_id="UnderArmourVietnam",
        post_collect_criterion="post_time",  # ["elapsed_minutes", "n_posts", "post_time"]
        post_collect_threshold=datetime(year=2024, month=9, day=1),
        # post_collect_criterion="n_posts",
        # post_collect_threshold=4,
    )
}
