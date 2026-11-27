"""Run in bot container with legacy routing stopped; defaults to dry run."""
import argparse
import asyncio
import json
from pathlib import Path
from tgteacher_bot.core.paths import help_index_path
from tgteacher_bot.db import web_operations as ops
from tgteacher_bot.db.pool import get_pool


async def main(args):
    with Path(help_index_path()).open("rb") as stream:
        raw=stream.read(32*1024*1024+1)
    pool=await get_pool()
    try:
        print(json.dumps(await ops.import_legacy_help(raw,args.group_id,args.apply)))
    finally:
        await pool.close()


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id",type=int,required=True)
    parser.add_argument("--apply",action="store_true")
    asyncio.run(main(parser.parse_args()))
