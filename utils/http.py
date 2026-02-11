from __future__ import annotations

import requests

from errors.network import NetworkError

DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CHALLENGE_MARKERS = (
    "Verifying that you are not a robot",
    "DDoS protection by Cloudflare",
)


def build_mirror_url(url: str) -> str:
    if url.startswith("https://"):
        return "https://r.jina.ai/http://" + url[len("https://") :]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://") :]
    return "https://r.jina.ai/http://" + url


def fetch_html(url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> str:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        mirror = build_mirror_url(url)
        try:
            mirror_response = requests.get(mirror, headers=headers, timeout=timeout)
        except requests.RequestException as mirror_exc:
            raise NetworkError(
                f"Request failed for {url}: {exc}; mirror failed: {mirror_exc}"
            ) from mirror_exc
        if mirror_response.status_code != 200:
            raise NetworkError(
                f"Request failed for {url}; mirror status {mirror_response.status_code}"
            )
        return mirror_response.text
    if response.status_code != 200:
        raise NetworkError(f"Unexpected status code {response.status_code} for {url}")

    html = response.text
    if any(marker in html for marker in CHALLENGE_MARKERS):
        mirror = build_mirror_url(url)
        try:
            mirror_response = requests.get(mirror, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            raise NetworkError(f"Challenge page received; mirror failed: {exc}") from exc
        if mirror_response.status_code != 200:
            raise NetworkError(
                f"Challenge page received; mirror status {mirror_response.status_code}"
            )
        return mirror_response.text
    return html
