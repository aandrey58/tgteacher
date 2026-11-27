"""Explicitly configured TTS adapters. No caller-controlled URLs or credentials."""
import os
import asyncio
import json
import hashlib
import tempfile
from pathlib import Path
import re
from dataclasses import dataclass, field
from io import BytesIO

import httpx
from mutagen.mp3 import MP3, HeaderNotFoundError

MAX_AUDIO_BYTES = 5 * 1024 * 1024
MAX_TEXT_BYTES = 2000
MAX_SOURCE_BYTES = 24 * 1024 * 1024
GATEWAYS = {'openrouter': 'https://openrouter.ai/api/v1', 'kodikrouter': 'https://api.kodikrouter.ru/v1', 'timeweb': 'https://api.timeweb.ai/v1'}
KEYS = {'openrouter': 'OPENROUTER_API_KEY', 'kodikrouter': 'KODIK_API_KEY', 'timeweb': 'TIMEWEB_AI_TOKEN'}
FORMATS = {'mp3', 'pcm', 'wav', 'opus', 'aac', 'flac'}
VOICE = re.compile(r'[A-Za-z0-9_.:-]{1,128}\Z')
MODEL = re.compile(r'[A-Za-z0-9_.:/-]{1,160}\Z')


@dataclass(frozen=True)
class Provider:
    name: str
    model: str
    voices: tuple[str, ...]
    rate_microusd_per_byte: int
    api_key: str = field(repr=False)

    source_format: str = 'mp3'
    pcm_rate: int = 0
    pcm_channels: int = 1

    async def synthesize(self, text, voice):
        if self.name == "openai":
            url = "https://api.openai.com/v1/audio/speech"
            headers = {"Authorization": "Bearer " + self.api_key}
            payload = {"model": self.model, "voice": voice, "input": text, "response_format": "mp3"}
        elif self.name == "elevenlabs":
            url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice}?output_format=mp3_44100_128"
            headers = {"xi-api-key": self.api_key}
            payload = {"model_id": self.model, "text": text}
        elif self.name in GATEWAYS:
            url = GATEWAYS[self.name] + "/audio/speech"
            headers = {"Authorization": "Bearer " + self.api_key}
            payload = {"model": self.model, "voice": voice, "input": text, "response_format": self.source_format}
            if self.name == "kodikrouter":
                headers["Idempotency-Key"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        else:
            raise ValueError("Unknown provider adapter")
        # No automatic retry: timeout may mean the upstream request was billed.
        async with httpx.AsyncClient(timeout=90, follow_redirects=False) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                response.raise_for_status()
                if self.source_format != 'mp3' and not response.headers.get('content-type', '').lower().startswith(('audio/', 'application/octet-stream')):
                    raise ValueError('Expected binary audio response')
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(chunks) + len(chunk) > MAX_SOURCE_BYTES:
                        raise ValueError("Audio exceeds size limit")
                    chunks.extend(chunk)
        data = bytes(chunks)
        return await convert_mp3(data, self.source_format, self.pcm_rate, self.pcm_channels) if self.source_format != 'mp3' else data


def configured_providers():
    providers = {}
    for name, model, default_voices, key_env in (
        ("openai", "tts-1", "alloy,echo,fable,onyx,nova,shimmer", "OPENAI_API_KEY"),
        ("elevenlabs", "eleven_multilingual_v2", "", "ELEVENLABS_API_KEY"),
    ):
        prefix = "TGTEACHER_TTS_" + name.upper()
        key = os.getenv(key_env, "").strip()
        rate = os.getenv(prefix + "_MICROUSD_PER_BYTE", "")
        voices = tuple(dict.fromkeys(v.strip() for v in os.getenv(prefix + "_VOICES", default_voices).split(",") if v.strip()))
        if not key or not rate or not voices:
            continue
        price = int(rate)
        if not 1 <= price <= 100000 or any(not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", v) for v in voices):
            raise ValueError("Invalid TTS provider configuration")
        providers[name] = Provider(name, model, voices, price, key)
    # Explicit administrator tariffs and format profiles enable paid calls.
    # Discovery alone never enables an unknown-price model.
    raw = json.loads(os.getenv("TGTEACHER_TTS_GATEWAY_MODELS", "{}"))
    if not isinstance(raw, dict) or set(raw) - set(GATEWAYS):
        raise ValueError("Invalid gateway model configuration")
    for name, models in raw.items():
        api_key = os.getenv(KEYS[name], "").strip()
        if not api_key:
            continue
        if not isinstance(models, dict) or len(models) > 100:
            raise ValueError("Invalid gateway model list")
        for model, cfg in models.items():
            if not MODEL.fullmatch(model) or not isinstance(cfg, dict):
                raise ValueError("Invalid gateway model")
            price = cfg.get("microusd_per_byte")
            voices = cfg.get("voices", [])
            fmt = cfg.get("format", "mp3")
            rate, channels = cfg.get("pcm_rate", 0), cfg.get("pcm_channels", 1)
            allowed = FORMATS if name == "timeweb" else {"mp3", "pcm"}
            if (type(price) is not int or not 1 <= price <= 100000
                    or not isinstance(voices, list) or any(not isinstance(v, str) or not VOICE.fullmatch(v) for v in voices)
                    or fmt not in allowed or (fmt == "pcm" and (type(rate) is not int or not 8000 <= rate <= 48000 or channels not in (1, 2)))):
                raise ValueError("Invalid gateway tariff/voice/format profile")
            providers[(name, model)] = Provider(name, model, tuple(dict.fromkeys(voices)), price, api_key, fmt, rate, channels)
    return providers


async def convert_mp3(data, fmt, rate=0, channels=1):
    if fmt == "mp3":
        return check_mp3(data)
    if fmt not in FORMATS or not data or len(data) > MAX_SOURCE_BYTES:
        raise ValueError("Invalid source audio")
    args = ["ffmpeg", "-nostdin", "-v", "error", "-protocol_whitelist", "file,pipe"]
    if fmt == "pcm":
        if not 8000 <= rate <= 48000 or channels not in (1, 2) or len(data) % (2*channels):
            raise ValueError("Invalid signed 16-bit little-endian PCM profile")
        args += ["-f", "s16le", "-ar", str(rate), "-ac", str(channels)]
    else:
        args += ["-f", {"opus": "ogg", "aac": "aac", "wav": "wav", "flac": "flac"}[fmt]]
    with tempfile.TemporaryDirectory() as tmp:
        source, output = Path(tmp)/"input", Path(tmp)/"output.mp3"
        source.write_bytes(data)
        proc = await asyncio.create_subprocess_exec(*args, "-i", str(source), "-vn", "-map", "0:a:0", "-t", "601",
                    "-codec:a", "libmp3lame", "-b:a", "64k", "-ac", "1", "-ar", "24000",
                    "-fs", str(MAX_AUDIO_BYTES), str(output), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.wait(), 20)
        except BaseException:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            raise
        if proc.returncode or not output.is_file() or output.stat().st_size >= MAX_AUDIO_BYTES:
            raise ValueError("Audio conversion failed or exceeds size limit")
        return check_mp3(output.read_bytes())


async def discover_catalog(name):
    """Public speech-only discovery. Timeweb has no verified speech-specific catalog."""
    if name == "timeweb":
        return [{"model": "openai/gpt-4o-mini-tts", "voices": "alloy,ash,ballad,coral,echo,fable,nova,onyx,sage,shimmer,verse,marin,cedar".split(","),
                 "formats": sorted(FORMATS), "source": "documented"}]
    if name not in ("openrouter", "kodikrouter"):
        return []
    url = GATEWAYS[name] + ("/audio/speech/models" if name == "openrouter" else "/catalog/models")
    rows = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        for offset in range(0, 500, 100):
            params = {} if name == "openrouter" else {"api_surfaces": "speech", "limit": 100, "offset": offset}
            async with client.stream("GET", url, params=params) as response:
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > 4*1024*1024:
                        raise ValueError("Catalog too large")
                    data.extend(chunk)
            body = json.loads(data)
            page = body["data" if name == "openrouter" else "items"]
            if not isinstance(page, list):
                raise ValueError("Unexpected catalog schema")
            for item in page:
                model = item.get("id")
                if not isinstance(model, str) or not MODEL.fullmatch(model):
                    continue
                supported = item.get("supported_parameters", {})
                if name == "openrouter":
                    supported = next((m.get("supported_parameters", {}) for m in item.get("output_modalities", []) if m.get("type") == "speech"), {})
                if not isinstance(supported, dict):
                    supported = {}
                def enum(key):
                    spec = supported.get(key, {})
                    values = spec.get("values", []) if isinstance(spec, dict) else []
                    return [v for v in values if isinstance(v, str)] if isinstance(values, list) else []
                rows.append({"model": model, "voices": [v for v in enum("voice") if VOICE.fullmatch(v)],
                             "formats": [v for v in enum("response_format") if v in FORMATS], "source": "live"})
            if name == "openrouter" or offset + len(page) >= body.get("total", 0) or not page:
                break
    return rows


def daily_limit():
    value = int(os.getenv("TGTEACHER_TTS_DAILY_MICROUSD", "0"))
    if not 0 <= value <= 1000000000:
        raise ValueError("Invalid TTS daily budget")
    return value


def check_mp3(data):
    if not isinstance(data, bytes) or not 128 <= len(data) <= MAX_AUDIO_BYTES:
        raise ValueError("Invalid MP3 size")
    try:
        audio = MP3(BytesIO(data))
        if not 0 < audio.info.length <= 600 or audio.info.layer != 3:
            raise ValueError("Invalid MP3 duration or codec")
    except (HeaderNotFoundError, ValueError, OSError) as exc:
        raise ValueError("Invalid MP3 response") from exc
    return data
