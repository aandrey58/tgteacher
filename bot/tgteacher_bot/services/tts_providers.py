"""Explicitly configured TTS adapters. No caller-controlled URLs or credentials."""
import os
import re
from dataclasses import dataclass, field
from io import BytesIO

import httpx
from mutagen.mp3 import MP3, HeaderNotFoundError

MAX_AUDIO_BYTES = 5 * 1024 * 1024
MAX_TEXT_BYTES = 2000


@dataclass(frozen=True)
class Provider:
    name: str
    model: str
    voices: tuple[str, ...]
    rate_microusd_per_byte: int
    api_key: str = field(repr=False)

    async def synthesize(self, text, voice):
        if self.name == "openai":
            url = "https://api.openai.com/v1/audio/speech"
            headers = {"Authorization": "Bearer " + self.api_key}
            payload = {"model": self.model, "voice": voice, "input": text, "response_format": "mp3"}
        elif self.name == "elevenlabs":
            url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice}?output_format=mp3_44100_128"
            headers = {"xi-api-key": self.api_key}
            payload = {"model_id": self.model, "text": text}
        else:
            raise ValueError("Unknown provider adapter")
        # No automatic retry: timeout may mean the upstream request was billed.
        async with httpx.AsyncClient(timeout=45, follow_redirects=False) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                response.raise_for_status()
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(chunks) + len(chunk) > MAX_AUDIO_BYTES:
                        raise ValueError("Audio exceeds size limit")
                    chunks.extend(chunk)
        return bytes(chunks)


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
    return providers


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
