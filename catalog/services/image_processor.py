"""
Image Processor — standardizes logos to square WebP with metadata stripped.

Strategy:
1. Strip metadata (EXIF, ICC, etc.)
2. Detect background colour (white, near-white, or transparent)
3. Auto-crop to content bounding box (trim away uniform background)
4. Resize to fit within the inner area (80% of canvas)
5. Pad to a square canvas with the detected background
6. Convert to WebP
"""
import io
import logging

from PIL import Image, ImageStat

logger = logging.getLogger(__name__)

LOGO_MAX_SIZE = 400
LOGO_QUALITY = 85
LOGO_PADDING_PERCENT = 10
# Near-background threshold — pixels within this distance of the detected
# background colour are considered background when auto-cropping.
BG_TOLERANCE = 30


def process_logo(image_data: bytes) -> tuple[bytes, dict]:
    """
    Process a raw logo image into a standardized 400×400 WebP.

    Returns (webp_bytes, metadata_dict).
    """
    img = Image.open(io.BytesIO(image_data))

    # Step 1: strip metadata by re-creating pixel data
    img = _strip_metadata(img)

    # Step 2: detect background and decide canvas colour
    bg_color, is_dark_logo = _detect_background(img)

    # Step 3: auto-crop to content bounding box
    img = _autocrop(img, bg_color)

    # Step 4: resize to fit within inner area
    inner = int(LOGO_MAX_SIZE * (1 - 2 * LOGO_PADDING_PERCENT / 100))
    w, h = img.size
    if w > inner or h > inner:
        ratio = min(inner / w, inner / h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)

    # Step 5: pad to square
    canvas_bg = (40, 40, 40) if is_dark_logo else (255, 255, 255)
    canvas = Image.new("RGB", (LOGO_MAX_SIZE, LOGO_MAX_SIZE), canvas_bg)
    x = (LOGO_MAX_SIZE - img.width) // 2
    y = (LOGO_MAX_SIZE - img.height) // 2
    if img.mode == "RGBA":
        canvas.paste(img, (x, y), img.split()[3])
    else:
        canvas.paste(img, (x, y))

    # Step 6: convert to WebP
    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=LOGO_QUALITY, method=6)
    webp_bytes = buf.getvalue()

    meta = {
        "width": LOGO_MAX_SIZE,
        "height": LOGO_MAX_SIZE,
        "format": "webp",
        "file_size": len(webp_bytes),
    }
    logger.info(
        f"Processed logo: {w}x{h} -> crop -> {img.width}x{img.height} "
        f"-> {LOGO_MAX_SIZE}x{LOGO_MAX_SIZE} ({meta['file_size']:,} bytes)"
    )
    return webp_bytes, meta


def _strip_metadata(img):
    """Re-create the image with only pixel data — strips EXIF, ICC, etc."""
    if img.mode in ("RGBA", "LA", "PA"):
        clean = Image.new("RGBA", img.size)
        clean.paste(img)
        return clean
    if img.mode == "P":
        return img.convert("RGBA") if "transparency" in img.info else img.convert("RGB")
    clean = Image.new("RGB", img.size)
    clean.paste(img.convert("RGB"))
    return clean


def _detect_background(img):
    """
    Detect the background colour and whether the logo is light-on-transparent.

    Samples the four corner regions (5×5 each). If ≥3 corners agree on colour,
    that's the background. For RGBA images with significant transparency,
    checks if the visible content is very bright (needs dark background).

    Returns (bg_color_rgb_tuple, is_dark_logo_bool).
    """
    # Handle transparent images
    if img.mode == "RGBA":
        brightness, visible_ratio = _analyze_transparency(img)
        if visible_ratio < 0.90 and brightness > 200:
            # Light content on transparency → needs dark canvas
            return (255, 255, 255), True
        if visible_ratio < 0.90:
            # Content on transparency, normal brightness → white canvas
            return (0, 0, 0), False

    # Sample 5×5 corners to detect background colour
    w, h = img.size
    rgb = img.convert("RGB")
    sample_size = min(5, w // 4, h // 4) or 1
    corners = [
        (0, 0, sample_size, sample_size),                           # top-left
        (w - sample_size, 0, w, sample_size),                       # top-right
        (0, h - sample_size, sample_size, h),                       # bottom-left
        (w - sample_size, h - sample_size, w, h),                   # bottom-right
    ]

    corner_colors = []
    for box in corners:
        region = rgb.crop(box)
        stat = ImageStat.Stat(region)
        avg = tuple(int(c) for c in stat.mean[:3])
        corner_colors.append(avg)

    # Find the most common corner colour (within tolerance)
    bg = corner_colors[0]
    for candidate in corner_colors:
        matches = sum(
            1 for c in corner_colors
            if all(abs(a - b) <= BG_TOLERANCE for a, b in zip(candidate, c))
        )
        if matches >= 3:
            bg = candidate
            break

    return bg, False


def _autocrop(img, bg_color):
    """
    Crop away uniform background to find the content bounding box.

    Works for both RGB and RGBA images. For RGBA, transparent pixels
    are treated as background regardless of bg_color.
    """
    w, h = img.size
    if w <= 1 or h <= 1:
        return img

    rgb = img.convert("RGB")
    pixels = list(rgb.getdata())

    # Also check alpha if present
    has_alpha = img.mode == "RGBA"
    alpha_data = list(img.split()[3].getdata()) if has_alpha else None

    bg_r, bg_g, bg_b = bg_color
    tolerance = BG_TOLERANCE

    def is_background(idx):
        if has_alpha and alpha_data[idx] < 32:
            return True
        r, g, b = pixels[idx]
        return (
            abs(r - bg_r) <= tolerance
            and abs(g - bg_g) <= tolerance
            and abs(b - bg_b) <= tolerance
        )

    # Scan for content bounding box
    top = bottom = left = right = None
    for y in range(h):
        for x in range(w):
            idx = y * w + x
            if not is_background(idx):
                if top is None:
                    top = y
                bottom = y
                if left is None or x < left:
                    left = x
                if right is None or x > right:
                    right = x

    # No content found — return as-is
    if top is None:
        return img

    # Add a small margin (2px or 1% of the dimension, whichever is larger)
    content_w = right - left + 1
    content_h = bottom - top + 1
    margin_x = max(2, int(content_w * 0.01))
    margin_y = max(2, int(content_h * 0.01))

    crop_left = max(0, left - margin_x)
    crop_top = max(0, top - margin_y)
    crop_right = min(w, right + 1 + margin_x)
    crop_bottom = min(h, bottom + 1 + margin_y)

    cropped = img.crop((crop_left, crop_top, crop_right, crop_bottom))

    logger.debug(
        f"Auto-crop: {w}x{h} -> {cropped.width}x{cropped.height} "
        f"(content was {content_w}x{content_h}, bg=({bg_r},{bg_g},{bg_b}))"
    )
    return cropped


def _analyze_transparency(img):
    """Analyze brightness of visible pixels in an RGBA image."""
    data = img.getdata()
    total = len(data)
    brights = []
    for r, g, b, a in data:
        if a > 128:
            brights.append(0.299 * r + 0.587 * g + 0.114 * b)
    if not brights:
        return 255.0, 0.0
    return sum(brights) / len(brights), len(brights) / total
