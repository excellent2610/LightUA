from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode, urlsplit

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db.database import Database
from db.models import AdCampaign

logger = logging.getLogger(__name__)

AD_DELIVERY_STATE_MAX_ATTEMPTS = 5
AD_RESERVATION_CANCEL_MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class AdItem:
    file: str
    enabled: bool
    campaign_id: str | None = None


@dataclass(frozen=True)
class AdContent:
    text: str
    button_text: str
    url: str
    id: str = ""
    name: str = ""
    enabled: bool = True


class AdManager:
    """Loads campaign JSON and delegates durable delivery accounting to PostgreSQL."""

    def __init__(
        self,
        config_path: str | Path = "add/config.json",
        db: Database | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.db = db
        self._synced_campaigns: tuple[AdCampaign, ...] | None = None
        self._last_ad_id: str | None = None
        self._last_ad_file: str | None = None
        self._tracking_base_url = self._load_tracking_base_url()

    @property
    def click_tracking_enabled(self) -> bool:
        return self._tracking_base_url is not None

    @staticmethod
    def _load_tracking_base_url() -> str | None:
        value = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
        if not value:
            return None
        try:
            parsed = urlsplit(value)
            parsed.port
        except ValueError:
            logger.error("PUBLIC_BASE_URL is invalid; click tracking is disabled")
            return None
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            logger.error(
                "PUBLIC_BASE_URL must be an absolute HTTPS origin; click tracking is disabled"
            )
            return None
        return value

    def _safe_load_json(self, path: Path) -> Optional[dict[str, Any]]:
        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError):
            logger.exception("Could not load advertising configuration file %s", path)
            return None
        if isinstance(data, dict):
            return data
        logger.error("Advertising configuration file %s must contain a JSON object", path)
        return None

    @staticmethod
    def _positive_int(value: object, default: int) -> int:
        try:
            converted = int(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return converted if converted >= 0 else default

    def load_config(self) -> dict[str, Any]:
        """Load the advertising settings while preserving legacy config files."""
        data = self._safe_load_json(self.config_path) or {}
        show_every = self._positive_int(data.get("show_every", 0), 0)
        max_per_user_per_day = self._positive_int(
            data.get("max_per_user_per_day", 3), 3
        )
        try:
            min_interval_hours = float(data.get("min_interval_hours", 4))
        except (TypeError, ValueError, OverflowError):
            min_interval_hours = 4.0
        if not math.isfinite(min_interval_hours) or min_interval_hours < 0:
            min_interval_hours = 4.0

        ads: list[AdItem] = []
        ads_raw = data.get("ads", [])
        if isinstance(ads_raw, list):
            for item in ads_raw:
                if not isinstance(item, dict):
                    continue
                file = item.get("file")
                if not isinstance(file, str) or not file.strip():
                    continue
                campaign_id = item.get("id")
                ads.append(
                    AdItem(
                        file=file.strip(),
                        enabled=bool(item.get("enabled", False)),
                        campaign_id=campaign_id.strip()
                        if isinstance(campaign_id, str) and campaign_id.strip()
                        else None,
                    )
                )

        return {
            "ads_enabled": bool(data.get("ads_enabled", False)),
            "show_every": show_every,
            "max_per_user_per_day": max_per_user_per_day,
            "min_interval_hours": min_interval_hours,
            "ads": ads,
        }

    def load_ad(self, ad_file: str | Path, campaign_id: str | None = None) -> Optional[AdContent]:
        """Load a legacy or extended campaign JSON file."""
        path = Path(ad_file)
        if not path.is_absolute():
            path = self.config_path.parent / path
        data = self._safe_load_json(path)
        if data is None:
            return None

        text = data.get("text")
        button_text = data.get("button_text")
        url = data.get("url")
        if not all(isinstance(value, str) and value.strip() for value in (text, button_text, url)):
            logger.error("Campaign file %s must define text, button_text, and url", path)
            return None
        try:
            parsed_url = urlsplit(url.strip())
        except ValueError:
            logger.error("Campaign file %s has an invalid advertiser URL", path)
            return None
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            logger.error("Campaign file %s has an invalid advertiser URL", path)
            return None

        raw_id = data.get("id") or campaign_id or path.stem
        if (
            not isinstance(raw_id, str)
            or not raw_id.strip()
            or len(raw_id.strip()) > 128
        ):
            logger.error("Campaign file %s has no valid id", path)
            return None
        name = data.get("name")
        if not isinstance(name, str) or not name.strip():
            name = raw_id
        return AdContent(
            id=raw_id.strip(),
            name=name.strip(),
            text=text.strip(),
            button_text=button_text.strip(),
            url=url.strip(),
            enabled=bool(data.get("enabled", True)),
        )

    def _load_campaigns(self, ads: list[AdItem]) -> list[tuple[AdItem, AdContent]]:
        campaigns: list[tuple[AdItem, AdContent]] = []
        seen_ids: set[str] = set()
        for item in ads:
            content = self.load_ad(item.file, item.campaign_id)
            if content is None:
                continue
            if content.id in seen_ids:
                logger.error("Duplicate advertising campaign id %r; skipping %s", content.id, item.file)
                continue
            seen_ids.add(content.id)
            campaigns.append((item, content))
        return campaigns

    def _get_available_ads(self, ads: list[AdItem]) -> list[tuple[str, AdContent]]:
        """Keep the original helper available to callers using AdManager directly."""
        return [
            (item.file, content)
            for item, content in self._load_campaigns(ads)
            if item.enabled and content.enabled
        ]

    def _pick_ad(
        self, available: list[tuple[str, AdContent]]
    ) -> tuple[str, AdContent] | None:
        """Pick randomly while avoiding the last campaign when alternatives exist."""
        if not available:
            return None
        alternatives = [item for item in available if item[0] != self._last_ad_file]
        picked = random.choice(alternatives or available)
        self._last_ad_file = picked[0]
        return picked

    async def _sync_campaigns(self, campaigns: list[tuple[AdItem, AdContent]]) -> None:
        if self.db is None:
            return
        records = tuple(
            AdCampaign(
                id=content.id,
                name=content.name,
                enabled=item.enabled and content.enabled,
                button_text=content.button_text,
                destination_url=content.url,
            )
            for item, content in campaigns
        )
        if records == self._synced_campaigns:
            return
        await self.db.sync_ad_campaigns(list(records))
        self._synced_campaigns = records

    async def sync_campaigns_from_config(
        self,
        config: dict[str, Any] | None = None,
    ) -> list[tuple[AdItem, AdContent]]:
        cfg = config if config is not None else self.load_config()
        campaigns = self._load_campaigns(cfg["ads"])
        await self._sync_campaigns(campaigns)
        return campaigns

    def _click_url(self, campaign: AdContent, telegram_id: int) -> str:
        if self._tracking_base_url is None or self.db is None:
            return campaign.url
        signature = self.db.sign_ad_click(campaign.id, telegram_id)
        path_id = quote(campaign.id, safe="")
        query = urlencode({"user_id": telegram_id, "signature": signature})
        return f"{self._tracking_base_url}/ads/click/{path_id}?{query}"

    async def _send_main(
        self,
        bot: Bot,
        chat_id: int,
        text: str,
        *,
        parse_mode: str | None,
        disable_web_page_preview: bool,
        kwargs: dict[str, Any],
    ) -> None:
        await bot.send_message(
            chat_id,
            text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            **kwargs,
        )

    async def _cancel_reservation(self, reservation_id: uuid.UUID) -> bool:
        if self.db is None:
            return False
        for attempt in range(1, AD_RESERVATION_CANCEL_MAX_ATTEMPTS + 1):
            try:
                await self.db.cancel_ad_impression(reservation_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not release failed ad impression reservation "
                    "(reservation_id=%s, attempt=%s/%s)",
                    reservation_id,
                    attempt,
                    AD_RESERVATION_CANCEL_MAX_ATTEMPTS,
                )
                if attempt < AD_RESERVATION_CANCEL_MAX_ATTEMPTS:
                    await asyncio.sleep(min(2 ** (attempt - 1), 60))
            else:
                return True
        logger.error(
            "Could not release failed ad impression reservation after %s attempts "
            "(reservation_id=%s); it remains uncounted",
            AD_RESERVATION_CANCEL_MAX_ATTEMPTS,
            reservation_id,
        )
        return False

    async def _mark_impression_delivered(
        self, reservation_id: uuid.UUID, delivered_at: datetime
    ) -> bool:
        if self.db is None:
            return False
        for attempt in range(1, AD_DELIVERY_STATE_MAX_ATTEMPTS + 1):
            try:
                await self.db.mark_ad_impression_delivered(
                    reservation_id, delivered_at
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not persist confirmed ad delivery "
                    "(reservation_id=%s, attempt=%s/%s)",
                    reservation_id,
                    attempt,
                    AD_DELIVERY_STATE_MAX_ATTEMPTS,
                )
                if attempt < AD_DELIVERY_STATE_MAX_ATTEMPTS:
                    await asyncio.sleep(min(2 ** (attempt - 1), 60))
            else:
                return True
        logger.error(
            "Confirmed ad delivery remains in delivery_outcome_unknown after "
            "%s attempts (reservation_id=%s); it was not counted",
            AD_DELIVERY_STATE_MAX_ATTEMPTS,
            reservation_id,
        )
        return False

    async def send_with_ad(
        self,
        bot: Bot,
        chat_id: int,
        text: str,
        *,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        telegram_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Send and count an ad after Telegram accepts it.

        A Telegram transport exception can have an ambiguous outcome: this keeps
        the existing behavior of releasing the reservation and retrying main-only,
        so a remotely accepted ad may not be counted. If cancellation cleanup
        fails, or this coroutine is cancelled during Telegram send, the durable
        unknown reservation is left uncounted because delivery cannot be proven.
        """
        cfg = self.load_config()
        if self.db is None:
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        try:
            campaigns = await self.sync_campaigns_from_config(cfg)
        except Exception:
            logger.exception(
                "Could not synchronize advertising campaigns; sending the main message without an ad"
            )
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        if not cfg["ads_enabled"] or cfg["show_every"] <= 0:
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        reservation_id = None
        try:
            user_id = telegram_id if telegram_id is not None else chat_id
            reservation = await self.db.reserve_ad_impression(
                user_id,
                [
                    content.id
                    for item, content in campaigns
                    if item.enabled and content.enabled
                ],
                show_every=cfg["show_every"],
                max_per_user_per_day=cfg["max_per_user_per_day"],
                min_interval_hours=cfg["min_interval_hours"],
            )
            if reservation is None:
                await self._send_main(
                    bot,
                    chat_id,
                    text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                    kwargs=kwargs,
                )
                return

            campaign_id, reservation_id = reservation
            content_by_id = {content.id: content for _, content in campaigns}
            campaign = content_by_id[campaign_id]
            self._last_ad_id = campaign_id
            self._last_ad_file = next(
                item.file for item, content in campaigns if content.id == campaign_id
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=campaign.button_text,
                            url=self._click_url(campaign, user_id),
                        )
                    ]
                ]
            )
            ad_kwargs = {**kwargs, "reply_markup": keyboard}
        except Exception:
            logger.exception("Could not prepare advertisement; sending the main message without an ad")
            if reservation_id is not None:
                await self._cancel_reservation(reservation_id)
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        try:
            await self.db.mark_ad_impression_delivery_started(reservation_id)
        except Exception:
            logger.exception(
                "Could not persist ad delivery intent; sending the main message "
                "without an ad (reservation_id=%s)",
                reservation_id,
            )
            await self._cancel_reservation(reservation_id)
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        try:
            await bot.send_message(
                chat_id,
                f"{text}\n\n{campaign.text}",
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                **ad_kwargs,
            )
        except asyncio.CancelledError:
            logger.warning(
                "Ad delivery was cancelled with an unknown Telegram outcome; "
                "leaving reservation uncounted (reservation_id=%s)",
                reservation_id,
            )
            raise
        except Exception:
            logger.exception("Telegram could not deliver an ad message; retrying the main message without the ad")
            await self._cancel_reservation(reservation_id)
            await self._send_main(
                bot,
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                kwargs=kwargs,
            )
            return

        delivered_at = datetime.now(timezone.utc)
        await self._mark_impression_delivered(reservation_id, delivered_at)


def click_tracking_configured() -> bool:
    """Report whether a valid public origin is configured for click redirects."""
    return AdManager._load_tracking_base_url() is not None


@lru_cache(maxsize=8)
def get_ad_manager(
    config_path: str | Path = "add/config.json",
    db: Database | None = None,
) -> AdManager:
    """Return one manager per config/database pair."""
    return AdManager(config_path, db=db)
