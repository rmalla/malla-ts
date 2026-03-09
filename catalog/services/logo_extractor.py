"""
Logo Extractor — finds and downloads manufacturer logos from websites.

Extraction strategies (in priority order):
1. Structured data (JSON-LD)     (confidence: 0.95)
2. HTML <img> logos in header/nav  (confidence: 0.7–0.95)
3. apple-touch-icon               (confidence: 0.7)
4. <link rel="icon"> ≥192px       (confidence: 0.65)
5. og:image                       (confidence: 0.5)
6. favicon.ico                    (confidence: 0.3)

Robustness features:
- SSL verification fallback (retry with verify=False on cert errors)
- HTTP ↔ HTTPS fallback on connection failures
- Generic platform asset filtering (GoDaddy, Wix, Squarespace, etc.)
- Negative keyword filtering (partner, certification, sponsor, etc.)
- Minimum source dimension 64px (avoids upscaling tiny icons)
- First-party domain preference (penalizes third-party CDN images)
"""
import json
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
MIN_IMAGE_DIMENSION = 64  # Raised from 32 — avoids tiny favicons being upscaled

# Generic platform CDN hosts whose default assets are not real logos
_PLATFORM_HOSTS = re.compile(
    r"(wsimg\.com|wixstatic\.com|squarespace-cdn\.com|"
    r"shopify\.com/s/files/.*logo-default|"
    r"godaddy\.com|weebly\.com|jimdo\.com)",
    re.IGNORECASE,
)

# URL path segments that indicate non-logo images
_NEGATIVE_PATH_RE = re.compile(
    r"(partner|sponsor|client|certification|certified|certificate|"
    r"badge|award|accredit|seal-of|trust-?seal|"
    r"customer|vendor|association|member-of|"
    r"logo-default|pwa-app|"
    r"background|illustration|hero|banner|wallpaper|carousel|slider)",
    re.IGNORECASE,
)


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
        site_domain = self._domain_name(base_url)

        raw = []
        raw.extend(self._extract_structured_data(soup, base_url))
        raw.extend(self._extract_html_img_logos(soup, base_url, site_domain))
        raw.extend(self._extract_apple_touch_icons(soup, base_url))
        raw.extend(self._extract_link_icons(soup, base_url))
        raw.extend(self._extract_og_image(soup, base_url))
        raw.extend(self._extract_favicon(base_url))

        # Filter by confidence and negative patterns
        raw = [c for c in raw if c["confidence"] >= self.min_confidence]
        raw = [c for c in raw if not self._is_rejected(c["url"])]
        if not raw:
            return []

        raw.sort(key=lambda c: c["confidence"], reverse=True)

        # Deduplicate by normalized URL
        seen_urls = set()
        unique = []
        for c in raw:
            norm = c["url"].split("?")[0].split("#")[0]
            if norm not in seen_urls:
                seen_urls.add(norm)
                unique.append(c)

        # Download, validate, deduplicate by content hash
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

        # Sort by confidence, then by pixel area (prefer larger)
        validated.sort(
            key=lambda c: (c.confidence, self._pixel_area(c.image_bytes)),
            reverse=True,
        )
        logger.info(f"Extracted {len(validated)} logo candidates from {url}")
        return validated

    # ── Filtering ──

    def _is_rejected(self, url: str) -> bool:
        """Reject URLs matching generic platform assets or negative keywords."""
        if _PLATFORM_HOSTS.search(url):
            return True
        # Check the path portion only (not domain) for negative keywords
        path = urlparse(url).path
        if _NEGATIVE_PATH_RE.search(path):
            return True
        return False

    # ── Strategies ──

    def _extract_structured_data(self, soup, base_url):
        """Extract logo URL from JSON-LD structured data blocks."""
        out = []
        target_types = {
            "Organization", "LocalBusiness", "ProfessionalService",
            "Corporation", "WebSite",
        }

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Collect candidate objects — handle @graph arrays and top-level
            objects = []
            if isinstance(data, dict):
                if "@graph" in data and isinstance(data["@graph"], list):
                    objects.extend(data["@graph"])
                else:
                    objects.append(data)
            elif isinstance(data, list):
                objects.extend(data)

            for obj in objects:
                if not isinstance(obj, dict):
                    continue
                obj_type = obj.get("@type", "")
                # @type can be a string or list
                types = obj_type if isinstance(obj_type, list) else [obj_type]
                if not target_types.intersection(types):
                    continue

                # Prefer logo, fall back to image
                for field in ("logo", "image"):
                    value = obj.get(field)
                    if not value:
                        continue
                    # Value can be a string URL or an object with "url"
                    if isinstance(value, dict):
                        url = value.get("url")
                    elif isinstance(value, str):
                        url = value
                    elif isinstance(value, list):
                        # Take first item
                        first = value[0] if value else None
                        if isinstance(first, dict):
                            url = first.get("url")
                        elif isinstance(first, str):
                            url = first
                        else:
                            continue
                    else:
                        continue

                    if url:
                        out.append({
                            "url": urljoin(base_url, url),
                            "strategy": "json-ld",
                            "confidence": 0.95,
                        })
                    break  # Found logo/image for this object, move to next

        return out

    def _extract_apple_touch_icons(self, soup, base_url):
        out = []
        for link in soup.find_all("link", rel=lambda r: r and "apple-touch-icon" in r):
            href = link.get("href")
            if href:
                # apple-touch-icons are brand icons but often small;
                # rank below a confirmed header <img> logo
                out.append({
                    "url": urljoin(base_url, href),
                    "strategy": "apple-touch-icon",
                    "confidence": 0.7,
                })
        return out

    def _extract_html_img_logos(self, soup, base_url, site_domain):
        out = []
        logo_re = re.compile(r"logo", re.IGNORECASE)

        for img in soup.find_all("img"):
            src = img.get("src")
            if not src:
                continue

            classes = " ".join(img.get("class", []))
            img_id = img.get("id", "")
            alt = img.get("alt", "")
            full_url = urljoin(base_url, src)

            class_id = logo_re.search(classes) or logo_re.search(img_id)
            alt_match = logo_re.search(alt)
            src_match = logo_re.search(src)
            if not (class_id or alt_match or src_match):
                continue

            # Base confidence
            conf = 0.5

            # Strong signal: class/id/alt contains "logo"
            if class_id or alt_match:
                conf = 0.7

            # In header or nav — very likely the site logo
            in_header = img.find_parent(["header", "nav"])
            if in_header:
                conf += 0.2

            # First-party domain match — image hosted on same domain
            img_host = urlparse(full_url).hostname or ""
            site_host = urlparse(base_url).hostname or ""
            if self._hosts_match(img_host, site_host):
                conf += 0.05
            else:
                # Third-party hosted image — penalize (could be partner logo)
                conf -= 0.1

            out.append({
                "url": full_url,
                "strategy": "html-img",
                "confidence": max(0.1, min(conf, 1.0)),
            })
        return out

    def _extract_link_icons(self, soup, base_url):
        out = []
        for link in soup.find_all("link", rel=lambda r: r and ("icon" in r or "shortcut icon" in r)):
            if "apple-touch-icon" in link.get("rel", []):
                continue
            href = link.get("href")
            if href:
                conf = 0.6
                sizes = link.get("sizes", "")
                if sizes:
                    try:
                        w = int(sizes.split("x")[0])
                        conf = 0.65 if w >= 192 else (0.3 if w <= 32 else 0.55)
                    except (ValueError, IndexError):
                        pass
                out.append({
                    "url": urljoin(base_url, href),
                    "strategy": "link-icon",
                    "confidence": conf,
                })
        return out

    def _extract_og_image(self, soup, base_url):
        tag = soup.find("meta", property="og:image")
        if tag and tag.get("content"):
            return [{
                "url": urljoin(base_url, tag["content"]),
                "strategy": "og-image",
                "confidence": 0.5,
            }]
        return []

    def _extract_favicon(self, base_url):
        parsed = urlparse(base_url)
        return [{
            "url": f"{parsed.scheme}://{parsed.netloc}/favicon.ico",
            "strategy": "favicon",
            "confidence": 0.3,
        }]

    # ── Helpers ──

    def _fetch_page(self, url):
        """Fetch page HTML with fallbacks for SSL errors and scheme swaps."""
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        # Try 1: normal fetch
        result = self._try_fetch(url, verify=True)
        if result[0]:
            return result

        # Try 2: SSL verification disabled (expired certs)
        if url.startswith("https://"):
            result = self._try_fetch(url, verify=False)
            if result[0]:
                logger.info(f"Fetched {url} with SSL verification disabled")
                return result

        # Try 3: swap scheme (http ↔ https)
        if url.startswith("https://"):
            alt = "http://" + url[8:]
        else:
            alt = "https://" + url[7:]

        result = self._try_fetch(alt, verify=True)
        if result[0]:
            return result

        # Try 4: alt scheme without SSL verify
        if alt.startswith("https://"):
            result = self._try_fetch(alt, verify=False)
            if result[0]:
                return result

        logger.warning(f"All fetch attempts failed for {url}")
        return None, url

    def _try_fetch(self, url, verify=True):
        try:
            r = self.session.get(
                url, timeout=self.timeout, allow_redirects=True, verify=verify,
            )
            r.raise_for_status()
            return r.text, r.url
        except requests.exceptions.RequestException as e:
            logger.debug(f"Fetch failed {url} (verify={verify}): {e}")
            return None, url

    def _download_image(self, url):
        """Download image with SSL fallback."""
        result = self._try_download(url, verify=True)
        if result:
            return result
        # Retry without SSL verification
        if url.startswith("https://"):
            return self._try_download(url, verify=False)
        return None

    def _try_download(self, url, verify=True):
        try:
            r = self.session.get(
                url, timeout=self.timeout, stream=True, verify=verify,
            )
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

    def _hosts_match(self, host_a, host_b):
        """Check if two hosts belong to the same domain (ignoring www/cdn prefixes)."""
        def base(h):
            h = (h or "").lower()
            parts = h.split(".")
            # Take last 2 parts (e.g. "cdn.example.com" → "example.com")
            return ".".join(parts[-2:]) if len(parts) >= 2 else h
        return base(host_a) == base(host_b)
