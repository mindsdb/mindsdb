# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Union

from .text_content_block_param import TextContentBlockParam
from .image_url_content_block_param import ImageURLContentBlockParam
from .image_file_content_block_param import ImageFileContentBlockParam

__all__ = ["MessageContentPartParam"]

MessageContentPartParam = Union[ImageFileContentBlockParam, ImageURLContentBlockParam, TextContentBlockParam]
