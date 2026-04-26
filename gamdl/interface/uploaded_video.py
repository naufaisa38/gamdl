import asyncio
from collections.abc import Callable
import re
from typing import AsyncGenerator

import structlog

from .base import AppleMusicBaseInterface
from .constants import UPLOADED_VIDEO_QUALITY_RANK
from .enums import UploadedVideoQuality
from .exceptions import (
    GamdlInterfaceFormatNotAvailableError,
    GamdlInterfaceMediaNotStreamableError,
)
from .types import AppleMusicMedia, MediaFileFormat, MediaTags, StreamInfo, StreamInfoAv

logger = structlog.get_logger(__name__)


class AppleMusicUploadedVideoInterface:
    def __init__(
        self,
        base: AppleMusicBaseInterface,
        quality: UploadedVideoQuality = UploadedVideoQuality.BEST,
        ask_quality_function: Callable[[dict], dict | None] | None = None,
    ):
        self.base = base
        self.quality = quality
        self.ask_quality_function = ask_quality_function

    def _format_quality_label(self, quality: str) -> str:
        label_map = {
            "1080pHdVideo": "1080p HD Video",
            "720pHdVideo": "720p HD Video",
            "sdVideoWithPlusAudio": "SD Video + Plus Audio",
            "sdVideo": "SD Video",
            "sd480pVideo": "480p Video",
            "provisionalUploadVideo": "Provisional Upload Video",
        }
        if quality in label_map:
            return label_map[quality]

        return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", quality)

    def _get_best_stream_selection(self, metadata: dict) -> tuple[str, str]:
        best_quality = next(
            (
                quality
                for quality in UPLOADED_VIDEO_QUALITY_RANK
                if metadata["attributes"]["assetTokens"].get(quality)
            ),
            None,
        )
        return best_quality, metadata["attributes"]["assetTokens"][best_quality]

    async def _get_stream_selection_from_user(
        self, metadata: dict
    ) -> tuple[str | None, str | None]:
        if self.ask_quality_function:
            selected_quality = self.ask_quality_function(
                metadata["attributes"]["assetTokens"]
            )
            if asyncio.iscoroutine(selected_quality):
                selected_quality = await selected_quality

            asset_tokens = metadata["attributes"]["assetTokens"]
            if selected_quality in asset_tokens:
                return selected_quality, asset_tokens[selected_quality]

            if selected_quality in asset_tokens.values():
                for quality, stream_url in asset_tokens.items():
                    if stream_url == selected_quality:
                        return quality, stream_url

                return None, selected_quality

            return None, None

        return None, None

    async def _get_stream_selection(
        self,
        metadata: dict,
    ) -> tuple[str | None, str | None]:
        if self.quality == UploadedVideoQuality.BEST:
            return self._get_best_stream_selection(metadata)

        if self.quality == UploadedVideoQuality.ASK:
            return await self._get_stream_selection_from_user(metadata)

        return None, None

    async def get_stream_info(
        self,
        metadata: dict,
    ) -> StreamInfo | None:
        log = logger.bind(
            action="get_uploaded_video_stream_info", media_id=metadata["id"]
        )

        selected_quality, stream_url = await self._get_stream_selection(metadata)
        if not stream_url:
            log.debug("no_stream_url_available")

            return None

        stream_info = StreamInfoAv(
            file_format=MediaFileFormat.M4V,
            video_track=StreamInfo(
                stream_url=stream_url,
            ),
            quality_label=(
                self._format_quality_label(selected_quality)
                if selected_quality
                else "Unknown"
            ),
        )

        log.debug("success", stream_info=stream_info)

        return stream_info

    def get_tags(self, metadata: dict) -> MediaTags:
        log = logger.bind(action="get_uploaded_video_tags", media_id=metadata["id"])

        attributes = metadata["attributes"]
        upload_date = attributes.get("uploadDate")

        tags = MediaTags(
            artist=attributes.get("artistName"),
            date=self.base.parse_date(upload_date) if upload_date else None,
            title=attributes.get("name"),
            title_id=int(metadata["id"]),
            storefront=self.base.itunes_api.storefront_id,
        )

        log.debug("success", tags=tags)

        return tags

    async def get_media(
        self,
        media: AppleMusicMedia,
    ) -> AsyncGenerator[AppleMusicMedia, None]:
        if not media.media_metadata:
            media.media_metadata = (
                await self.base.apple_music_api.get_uploaded_video(media.media_id)
            )["data"][0]

        media.media_id = self.base.parse_catalog_media_id(media.media_metadata)

        yield media

        if not self.base.is_media_streamable(media.media_metadata):
            raise GamdlInterfaceMediaNotStreamableError(media.media_id)

        media.cover = await self.base.get_cover(media.media_metadata)

        media.stream_info = await self.get_stream_info(media.media_metadata)
        if not media.stream_info:
            raise GamdlInterfaceFormatNotAvailableError(media.media_id)

        media.tags = self.get_tags(media.media_metadata)

        media.partial = False

        yield media
