"""Bounded image generation through explicitly enabled gateway model profiles."""
import os
import json
import base64
import hashlib
import warnings
from dataclasses import dataclass, field
from io import BytesIO

import httpx
from PIL import Image, ImageOps
from tgteacher_bot.services.tts_providers import GATEWAYS, KEYS, MODEL

MAX_SOURCE = 16 * 1024 * 1024
MAX_JPEG = 5 * 1024 * 1024
MAX_RESPONSE = 24 * 1024 * 1024
SIZES = {"512x512", "1024x1024", "1536x1024", "1024x1536"}


def normalize_image(data):
    """Reject animation, bombs and unsupported formats; strip metadata."""
    if not isinstance(data, bytes) or not 1 <= len(data) <= MAX_SOURCE:
        raise ValueError("Invalid image size")
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(BytesIO(data), formats=["JPEG", "PNG", "WEBP"]) as im:
            if im.width * im.height > 16_000_000 or min(im.size) < 16 or getattr(im, "n_frames", 1) != 1:
                raise ValueError("Invalid image dimensions or animation")
            im.load()
            im = ImageOps.exif_transpose(im)
            im.thumbnail((1536, 1536))
            rgba = im.convert("RGBA")
            rgb = Image.new("RGB", rgba.size, "white")
            rgb.paste(rgba, mask=rgba.getchannel("A"))
            output = BytesIO()
            rgb.save(output, format="JPEG", quality=88, optimize=True)
            result = output.getvalue()
    if len(result) > MAX_JPEG:
        raise ValueError("Normalized image too large")
    return result, rgb.width, rgb.height


@dataclass(frozen=True)
class ImageProvider:
    name: str
    model: str
    size: str
    cost_microusd: int
    api_key: str = field(repr=False)
    surface: str = "images"

    async def generate(self, prompt):
        base = GATEWAYS[self.name]
        headers = {"Authorization": "Bearer " + self.api_key}
        if self.surface == "chat":
            if self.name != "openrouter":
                raise ValueError("Chat image generation supported only on OpenRouter")
            url = base + "/chat/completions"
            ratio = {"512x512": "1:1", "1024x1024": "1:1", "1536x1024": "3:2", "1024x1536": "2:3"}[self.size]
            payload = {"model": self.model, "messages": [{"role":"user","content":prompt}],
                       "modalities": ["image", "text"], "stream": False,
                       "image_config": {"aspect_ratio": ratio, "image_size": "1K"},
                       "provider": {"allow_fallbacks": False}}
        else:
            url = base + ("/images" if self.name == "openrouter" else "/images/generations")
            payload = {"model": self.model, "prompt": prompt, "n": 1, "size": self.size}
            if self.name != "openrouter":
                payload["response_format"] = "b64_json"
            if self.name == "openrouter":
                payload["provider"] = {"allow_fallbacks": False}
        if self.name == "kodikrouter":
            headers["Idempotency-Key"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        async with httpx.AsyncClient(timeout=150, follow_redirects=False) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError("Expected synchronous image response")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_RESPONSE:
                        raise ValueError("Image response too large")
                    body.extend(chunk)
        doc = json.loads(body)
        if self.surface == "chat":
            encoded = doc["choices"][0]["message"]["images"][0]["image_url"]["url"]
            prefix, sep, encoded = encoded.partition(",")
            if not sep or prefix not in ("data:image/png;base64", "data:image/jpeg;base64", "data:image/webp;base64"):
                raise ValueError("Expected image data URI, not a remote URL")
        else:
            encoded = doc["data"][0]["b64_json"]
        if not isinstance(encoded, str) or len(encoded) > (MAX_SOURCE*4//3+4):
            raise ValueError("Invalid base64 image")
        return base64.b64decode(encoded, validate=True)


def configured_models():
    raw = json.loads(os.getenv("TGTEACHER_IMAGE_MODELS", "{}"))
    if not isinstance(raw, dict) or set(raw) - set(GATEWAYS):
        raise ValueError("Invalid image model configuration")
    result = {}
    for name, models in raw.items():
        key = os.getenv(KEYS[name], "").strip()
        if not key:
            continue
        if not isinstance(models, dict) or len(models) > 100:
            raise ValueError("Invalid image models")
        for model, cfg in models.items():
            if not MODEL.fullmatch(model) or not isinstance(cfg, dict):
                raise ValueError("Invalid image model")
            if set(cfg) - {"microusd_per_image", "size", "surface"}:
                raise ValueError("Unknown image profile field")
            price, size, surface = cfg.get("microusd_per_image"), cfg.get("size", "1024x1024"), cfg.get("surface", "images")
            if type(price) is not int or not 1 <= price <= 100_000_000 or size not in SIZES or surface not in ("images", "chat") or (surface == "chat" and name != "openrouter"):
                raise ValueError("Invalid image tariff/size/surface profile")
            result[(name, model)] = ImageProvider(name, model, size, price, key, surface)
    return result


def daily_limit():
    limit = int(os.getenv("TGTEACHER_IMAGE_DAILY_MICROUSD", "0"))
    if not 0 <= limit <= 1_000_000_000:
        raise ValueError("Invalid image daily budget")
    return limit
