from __future__ import annotations

import json
import random
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


@dataclass(frozen=True)
class AdItem:
    file: str
    enabled: bool


@dataclass(frozen=True)
class AdContent:
    text: str
    button_text: str
    url: str


class AdManager:
    """
    Manages ad rotation and message counting.
    """

    def __init__(self, config_path: str | Path = "add/config.json") -> None:
        self.config_path = Path(config_path)
        self._message_counter = 0
        self._last_ad_file: Optional[str] = None

    def _safe_load_json(self, path: Path) -> Optional[dict[str, Any]]:
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def load_config(self) -> dict[str, Any]:
        """
        Load config.json safely. Returns defaults on error.
        """
        data = self._safe_load_json(self.config_path) or {}
        ads_enabled = bool(data.get("ads_enabled", False))
        show_every = data.get("show_every", 0)
        try:
            show_every = int(show_every)
        except Exception:
            show_every = 0

        ads_raw = data.get("ads", [])
        ads: list[AdItem] = []
        if isinstance(ads_raw, list):
            for item in ads_raw:
                if not isinstance(item, dict):
                    continue
                file = item.get("file")
                enabled = bool(item.get("enabled", False))
                if isinstance(file, str) and file.strip():
                    ads.append(AdItem(file=file, enabled=enabled))

        return {
            "ads_enabled": ads_enabled,
            "show_every": show_every,
            "ads": ads,
        }

    def load_ad(self, ad_file: str | Path) -> Optional[AdContent]:
        """
        Load ad JSON safely. Returns None on error or invalid structure.
        """
        path = Path(ad_file)
        if not path.is_absolute():
            # Resolve relative ad paths against config directory
            path = self.config_path.parent / path
        data = self._safe_load_json(path)
        if not data:
            return None

        text = data.get("text")
        button_text = data.get("button_text")
        url = data.get("url")
        if all(isinstance(v, str) and v.strip() for v in (text, button_text, url)):
            return AdContent(text=text.strip(), button_text=button_text.strip(), url=url.strip())
        return None

    def _get_available_ads(self, ads: list[AdItem]) -> list[tuple[str, AdContent]]:
        """
        Load enabled ads, skipping corrupted files.
        """
        available: list[tuple[str, AdContent]] = []
        for ad in ads:
            if not ad.enabled:
                continue
            content = self.load_ad(ad.file)
            if content is None:
                continue
            available.append((ad.file, content))
        return available

    def _pick_ad(self, available: list[tuple[str, AdContent]]) -> Optional[tuple[str, AdContent]]:
        """
        Pick random ad without repeating the previous one.
        """
        if not available:
            return None

        if self._last_ad_file is None or len(available) == 1:
            return random.choice(available)

        filtered = [item for item in available if item[0] != self._last_ad_file]
        if not filtered:
            # All ads are the same as last (edge case); allow repeat
            return random.choice(available)
        return random.choice(filtered)

    async def send_with_ad(
        self,
        bot: Bot,
        chat_id: int,
        text: str,
        *,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Send main text and optionally an ad based on config and counter.
        """
        self._message_counter += 1

        cfg = self.load_config()
        ads_enabled = cfg["ads_enabled"]
        show_every = cfg["show_every"]

        if not ads_enabled or show_every <= 0:
            await bot.send_message(
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                **kwargs,
            )
            return

        should_show = (self._message_counter % show_every) == 0
        if not should_show:
            await bot.send_message(
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                **kwargs,
            )
            return

        available = self._get_available_ads(cfg["ads"])
        picked = self._pick_ad(available)
        if picked is None:
            await bot.send_message(
                chat_id,
                text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                **kwargs,
            )
            return

        ad_file, ad = picked
        self._last_ad_file = ad_file

        full_text = f"{text}\n\n{ad.text}"
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=ad.button_text, url=ad.url)]
            ]
        )

        await bot.send_message(
            chat_id,
            full_text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=keyboard,
            **kwargs,
        )


@lru_cache(maxsize=1)
def get_ad_manager(config_path: str | Path = "add/config.json") -> AdManager:
    """
    Singleton-like accessor for AdManager to avoid counter reset on each handler call.
    """
    return AdManager(config_path)


# Example usage:
#
# ad_manager = AdManager("config.json")
#
# async def some_handler(message: Message, bot: Bot):
#     await ad_manager.send_with_ad(
#         bot,
#         message.chat.id,
#         "Основний текст бота",
#     )
