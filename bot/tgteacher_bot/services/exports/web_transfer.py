"""Bounded ZIP interchange and uploaded immutable assets, without filesystem extraction."""
import asyncio
import hashlib
import io
import stat
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from tgteacher_bot.core.paths import families_dir
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db import content_versions as versions, drafts_repo as drafts, image_repo as images, tts_repo as audio, media_repo as media
from tgteacher_bot.services.image_providers import normalize_image
from tgteacher_bot.services.tts_providers import check_mp3
from tgteacher_bot.utils.family_parser import parse_family_text
from tgteacher_bot.utils.family_renderer import render_family_txt
from tgteacher_bot.utils.content_validation import validate_family

MAX_ARCHIVE=32*1024*1024
MAX_TOTAL=64*1024*1024
MAX_FILE=16*1024*1024
MAX_ENTRIES=501


def prepare_asset(kind,data):
    if kind=="image":
        from PIL import Image
        try:
            data,w,h=normalize_image(data)
        except (OSError,Image.DecompressionBombError,Image.DecompressionBombWarning) as exc:
            raise ValueError('Invalid image') from exc
    elif kind=="audio":
        data=check_mp3(data)
        w=h=None
    else:
        raise ValueError("Unknown media kind")
    ident=hashlib.sha256(data).hexdigest()
    return {"kind":kind,"asset_id":ident,"filename":(images if kind=="image" else audio).asset_filename(ident),
            "data":data,"width":w,"height":h}


async def store_asset(c,asset):
    ident,data=asset["asset_id"],asset["data"]
    if asset["kind"]=="image":
        await c.execute("INSERT INTO image_assets(asset_id,jpeg,width,height) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",ident,data,asset["width"],asset["height"])
    else:
        await c.execute("INSERT INTO audio_assets(asset_id,mp3) VALUES($1,$2) ON CONFLICT DO NOTHING",ident,data)


async def upload_asset(kind,data):
    asset=await asyncio.to_thread(prepare_asset,kind,data)
    pool=await get_pool()
    async with pool.acquire() as c:
        await store_asset(c,asset)
    return {k:v for k,v in asset.items() if k!="data"}


def parse_archive(raw):
    if not 0<len(raw)<=MAX_ARCHIVE:
        raise ValueError("ZIP size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            files={}
            total=0
            entries=z.infolist()
            if len(entries)>MAX_ENTRIES:
                raise ValueError("Too many archive entries")
            for info in entries:
                path=PurePosixPath(info.filename)
                if (path.is_absolute() or ".." in path.parts or "\\" in info.filename or ":" in info.filename
                        or stat.S_ISLNK(info.external_attr>>16) or info.flag_bits & 1):
                    raise ValueError("Unsafe or encrypted archive")
                if info.is_dir():
                    continue
                if len(path.parts) not in (1,2) or not path.name or info.file_size>MAX_FILE:
                    raise ValueError("Invalid archive entry")
                total+=info.file_size
                if total>MAX_TOTAL:
                    raise ValueError("Expanded ZIP size limit")
                key=path.name.casefold()
                if key in files:
                    raise ValueError("Duplicate filename")
                files[key]=(path,z.read(info))
            txt=[(p,b) for p,b in files.values() if p.suffix.lower()==".txt"]
            if len(txt)!=1:
                raise ValueError("Expected one TXT lesson")
            source=drafts.check_source(txt[0][1].decode("utf-8-sig"))
            content=parse_family_text(source)
            validate_family(content)
            assets={}
            for kind,stage in (("image",5),("audio",7)):
                for task in content.get(f"stage{stage}_tasks",[]):
                    name=task[kind]
                    item=files.get(name.casefold())
                    if not item or item[0].parent!=txt[0][0].parent:
                        raise ValueError("Missing media in archive")
                    key=(kind,name.casefold())
                    if key not in assets:
                        assets[key]=prepare_asset(kind,item[1])
                    task[kind]=assets[key]["filename"]
            candidate=versions.clean_content(content)
            source=drafts.check_source(render_family_txt(candidate))
            return source,list(assets.values())
    except (zipfile.BadZipFile,UnicodeError,RuntimeError,NotImplementedError) as exc:
        raise ValueError("Invalid ZIP") from exc


async def import_archive(raw):
    source,assets=await asyncio.to_thread(parse_archive,raw)
    ident=uuid.uuid4()
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            for asset in assets:
                await store_asset(c,asset)
            await media.validate_media(c,parse_family_text(source))
            await c.execute("INSERT INTO content_drafts(id,family_txt) VALUES($1,$2)",ident,source)
    return {"draft_id":str(ident),"revision":1,"assets":len(assets)}


def read_legacy(folder,name):
    if not folder or Path(folder).name!=folder or Path(name).name!=name:
        raise ValueError("Invalid legacy path")
    root=families_dir().resolve()
    directory=root/folder
    if directory.is_symlink() or not directory.is_dir() or directory.resolve().parent!=root:
        raise ValueError("Invalid legacy folder")
    matches=[p for p in directory.iterdir() if p.name.casefold()==name.casefold()]
    if len(matches)!=1 or matches[0].is_symlink() or not matches[0].is_file():
        raise ValueError("Missing legacy media")
    with matches[0].open("rb") as f:
        data=f.read(MAX_FILE+1)
    if len(data)>MAX_FILE:
        raise ValueError("Legacy media exceeds size limit")
    return data


async def export_archive(family_id):
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction(isolation="repeatable_read",readonly=True):
            meta,content=await versions.read_content(c,family_id)
            source=render_family_txt(content).encode()
            entries={"lesson.txt":source}
            total=len(source)
            for kind,stage,mod,col,table in (("image",5,images,"jpeg","image_assets"),("audio",7,audio,"mp3","audio_assets")):
                for task in content.get(f"stage{stage}_tasks",[]):
                    name=task[kind]
                    if name in entries:
                        continue
                    if Path(name).name!=name or "\\" in name:
                        raise ValueError("Unsafe filename")
                    ident=mod.managed_id(name)
                    if ident:
                        data=await c.fetchval(f"SELECT {col} FROM {table} WHERE asset_id=$1",ident)
                        if data is None:
                            raise ValueError("Managed media missing")
                    else:
                        data=await asyncio.to_thread(read_legacy,meta["folder_name"],name)
                    total+=len(data)
                    if total>MAX_TOTAL or len(entries)>=MAX_ENTRIES:
                        raise ValueError("Export exceeds size limit")
                    entries[name]=data
    out=io.BytesIO()
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for name,data in entries.items():
            z.writestr(f"family-{family_id}/{name}",data)
    if out.tell()>MAX_ARCHIVE:
        raise ValueError("Export exceeds ZIP size limit")
    return out.getvalue()
