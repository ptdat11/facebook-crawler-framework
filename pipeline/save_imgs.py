from .base_step import BaseStep
from utils.utils import FormatablePath

import os
from os.path import join
import requests
from pandas import DataFrame
from pathlib import Path
from typing import Any


class SaveImages(BaseStep):
    def __init__(
        self,
        save_dir: str,
        img_url_col: str,
        img_name_format: str = "{post_id}_{cmt_id}_{ordinal}.jpg",
    ) -> None:
        self.img_url_col = img_url_col
        self.save_dir = FormatablePath(save_dir)
        self.img_name_format = img_name_format

    def save_img(self, url: str, post_id: str, cmt_id: str, ordinal: int):
        img_name = self.img_name_format.format(
            post_id=post_id, cmt_id=cmt_id, ordinal=ordinal
        )
        img_path = join(self.save_dir, img_name)
        img_data = requests.get(url).content
        if os.path.exists(img_path):
            return img_name

        with open(img_path, "wb") as f:
            f.write(img_data)
        return img_name

    def __call__(
        self,
        df: DataFrame,
    ) -> Any:
        if not df.empty:
            is_post = df["type"] == "post"

        if not self.save_dir.exists():
            os.makedirs(self.save_dir, exist_ok=True)

        for row in df.itertuples():
            post_img = df.loc[
                (df["post_id"] == row.post_id) & is_post, self.img_url_col
            ].values[0]

            img_files = []
            imgs = row.images.split()
            for i, url in enumerate(imgs):
                img_file = self.save_img(
                    url=url,
                    post_id=row.post_id,
                    cmt_id=(
                        row.cmt_id if getattr(row, self.img_url_col) != post_img else ""
                    ),
                    ordinal=i,
                )
                img_files.append(img_file)
            df.loc[row.Index, "image_paths"] = "   ".join(img_files)

        return df
