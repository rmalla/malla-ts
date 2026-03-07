"""
Logo Extractor — finds and downloads manufacturer logos from websites.

Extraction strategies (in priority order):
1. apple-touch-icon  (confidence: 0.9)
2. HTML <img> logos   (confidence: 0.8)
3. <link rel="icon">  (confidence: 0.7)
4. og:image           (confidence: 0.6)
5. favicon.ico        (confidence: 0.3)
"""
import hashlib
import io
import logging
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from PIL import Image

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
MAX_IMAGE_SIZE = 5 * 1024 * 1024
MIN_IMAGE_BYTES = 100
MIN_IMAGE_DIMENSION = 32


@dataclass
class LogoCandidate:
    image_bytes: bytes
    source_url: str
    strategy: str
    confidence: float
    content_type: str
    content_hash: str


class LogoExtractorService:
    def __init__(self, timeout=DEFAULT_TIMEOUT, min_confidence=0.5):
        self.timeout = timeout
        self.min_confidence = min_confidence
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/*;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def extract(self, url: str) -> list[LogoCandidate]:
        html, final_url = self._fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        base_url = final_url

        raw = []
        raw.extend(self._extract_apple_touch_icons(soup, base_url))
        raw.extend(self._extract_html_img_logos(soup, base_url))
        raw.extend(self._extract_link_icons(soup, base_url))
        raw.extend(self._extract_og_image(soup, base_url))
        raw.extend(self._extract_favicon(base_url))

        raw = [c for c in raw if c["confidence"] >= self.min_confidence]
        if not raw:
            return []

        raw.sort(key=lambda c: c["confidence"], reverse=True)

        seen_urls = set()
        unique = []
        for c in raw:
            norm = c["url"].split("?")[0].split("#")[0]
            if norm not in seen_urls:
                seen_urls.add(norm)
                unique.append(c)

        validated = []
        seen_hashes = set()
        for c in unique:
            result = self._download_image(c["url"])
            if result is None:
                continue
            image_bytes, content_type = result
            if not self._validate_image(image_bytes):
                continue
            h = hashlib.sha256(image_bytes).hexdigest()
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            validated.append(LogoCandidate(
                image_bytes=image_bytes,
                source_url=c["url"],
                strategy=c["strategy"],
                confidence=c["confidence"],
                content_type=content_type,
                content_hash=h,
            ))

        # Sort by confidence, then by pixel area (prefer larger images over file size)
        validated.sort(key=lambda c: (c.confidence, self._pixel_area(c.image_bytes)), reverse=True)
        logger.info(f"Extracted {len(validated)} logo candidates from {url}")
        return validated

    # ── Strategies ──

    def _extract_apple_touch_icons(self, soup, base_url):
        out = []
        for link in soup.find_all("link", rel=lambda r: r and "apple-touch-icon" in r):
            href = link.get("href")
            if href:
                out.append({"url": urljoin(base_url, href), "strategy": "apple-touch-icon", "confidence": 0.9})
        return out

    def _extract_html_img_logos(self, soup, base_url):
        out = []
        logo_re = re.compile(r"logo", re.IGNORECASE)
        domain = self._domain_name(base_url)
        first = False

        for img in soup.find_all("img"):
            src = img.get("src")
            if not src:
                continue
            classes = " ".join(img.get("class", []))
            img_id = img.get("id", "")
            alt = img.get("alt", "")
            class_id = logo_re.search(classes) or logo_re.search(img_id)
            alt_match = logo_re.search(alt)
            src_match = logo_re.search(src)
            if not (class_id or alt_match or src_match):
                continue

            conf = 0.5
            if class_id or alt_match:
                conf = 0.7
            if img.find_parent(["header", "nav"]):
                conf += 0.2
            if domain and re.search(re.escape(domain), src, re.IGNORECASE):
                conf += 0.1
            if not first and (class_id or alt_match):
                conf += 0.05
                first = True
            out.append({"url": urljoin(base_url, src), "strategy": "html-img", "confidence": min(conf, 1.0)})
        return out

    def _extract_link_icons(self, soup, base_url):
        out = []
        for link in soup.find_all("link", rel=lambda r: r and ("icon" in r or "shortcut icon" in r)):
            if "apple-touch-icon" in link.get("rel", []):
                continue
            href = link.get("href")
            if href:
                conf = 0.7
                sizes = link.get("sizes", "")
                if sizes:
                    try:
                        w = int(sizes.split("x")[0])
                        conf = 0.75 if w >= 192 else (0.4 if w <= 32 else 0.7)
                    except (ValueError, IndexError):
                        pass
                out.append({"url": urljoin(base_url, href), "strategy": "link-icon", "confidence": conf})
        return out

    def _extract_og_image(self, soup, base_url):
        tag = soup.find("meta", property="og:image")
        if tag and tag.get("content"):
            return [{"url": urljoin(base_url, tag["content"]), "strategy": "og-image", "confidence": 0.6}]
        return []

    def _extract_favicon(self, base_url):
        parsed = urlparse(base_url)
        return [{"url": f"{parsed.scheme}://{parsed.netloc}/favicon.ico", "strategy": "favicon", "confidence": 0.3}]

    # ── Helpers ──

    def _fetch_page(self, url):
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        try:
            r = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            r.raise_for_status()
            return r.text, r.url
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None, url

    def _download_image(self, url):
        try:
            r = self.session.get(url, timeout=self.timeout, stream=True)
            r.raise_for_status()
            chunks, total = [], 0
            for chunk in r.iter_content(chunk_size=8192):
                total += len(chunk)
                if total > MAX_IMAGE_SIZE:
                    return None
                chunks.append(chunk)
            data = b"".join(chunks)
            if len(data) < MIN_IMAGE_BYTES:
                return None
            return data, r.headers.get("Content-Type", "image/unknown")
        except requests.exceptions.RequestException:
            return None

    def _validate_image(self, data):
        try:
            img = Image.open(io.BytesIO(data))
            w, h = img.size
            return w >= MIN_IMAGE_DIMENSION and h >= MIN_IMAGE_DIMENSION
        except Exception:
            return False

    def _pixel_area(self, data):
        """Return pixel area (w*h) for sorting — larger images are preferred."""
        try:
            img = Image.open(io.BytesIO(data))
            return img.size[0] * img.size[1]
        except Exception:
            return 0

    def _domain_name(self, url):
        try:
            host = urlparse(url).hostname or ""
            return host.replace("www.", "").split(".")[0]
        except Exception:
            return None
