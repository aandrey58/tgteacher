"""Synthetic screenshot fixture, invoked ONLY by isolated release-smoke CI."""
import asyncio
import hashlib
import io
import os
import uuid
from PIL import Image, ImageDraw
from tgteacher_bot.db import user_repo as users, families_repo as families, drafts_repo as drafts, web_operations as ops
from tgteacher_bot.db.pool import get_pool

async def main():
    if os.getenv("POSTGRES_PASSWORD") != "ci-only-admin-runtime-cccccccc" or os.getenv("WEB_ADMIN_SECRET") != "ci-only-web-admin-"+"a"*32:
        raise RuntimeError("Refusing to seed a non-CI installation")
    await users.init_db_pg()
    pool = await get_pool()
    async with pool.acquire() as c:
        if await c.fetchval("SELECT count(*) FROM families"):
            raise RuntimeError("Expected empty isolated database")
    groups = [
        ("Travel essentials","FREE","Vocabulary for journeys and transport",["ticket","passport","luggage","departure"]),
        ("Food & cooking","VIP+FREE","Everyday ingredients and cooking",["recipe","vegetable","spice","meal"]),
        ("Work & career","VIP","Practical workplace English",["meeting","deadline","colleague","salary"]),
        ("Small talk","FREE","Start a friendly conversation",["weather","weekend","hobby","neighbor"]),
        ("Technology","VIP+FREE","Digital life and devices",["screen","keyboard","network","charger"]),
    ]
    translations = [
        ["билет","паспорт","багаж","отправление"],["рецепт","овощ","специя","приём пищи"],
        ["встреча","срок","коллега","зарплата"],["погода","выходные","хобби","сосед"],
        ["экран","клавиатура","сеть","зарядное устройство"]]
    ids=[]
    for i,(name,target,description,words) in enumerate(groups):
        ident=str(uuid.uuid4())
        source=('#FAMILY "'+name+'"\n#DESCRIPTION '+description+'\n#TARGET '+target+'\n'
                +''.join('#WORD '+word+'\n#TRANSLATION '+translation+'\n' for word,translation in zip(words,translations[i]))
                +'#STAGE2\n#TASK\n#SENTENCE Show me the ___.\n#ANSWER '+words[0]+'\n#CHOICES '+'; '.join(words)+'\n')
        # Create through the same content repository, avoiding paid APIs.
        await drafts.create_draft(ident,source)
        result=await drafts.publish_draft(ident,1)
        ids.append(result["family_id"])
    await drafts.create_draft(str(uuid.uuid4()),'#FAMILY "Everyday phrases"\n#WORD hello\n#TRANSLATION привет\n')
    for i,(first,last) in enumerate([("Alex","Demo"),("Sam","Demo"),("Taylor","Demo"),("Jordan","Demo")]):
        uid=900000000+i
        await users.add_or_update_user_pg(uid,"demo_"+first.lower(),first,last)
        await users.set_task_done_pg(uid,ids[i],1,0)
        await users.set_stage2_answer_pg(uid,ids[i],0,groups[i][3][0],True)
        if i==2:
            await users.extend_user_subscription_pg(uid,1)
    await ops.create_question(900000000,"CI role test question")
    # Locally generated illustrations are CI-only, never charged to a provider.
    async with pool.acquire() as c:
        for i,(label,color) in enumerate([("Ticket","#2563eb"),("Cooking","#0d9488"),("Work","#7c3aed")]):
            im=Image.new("RGB",(480,360),"#f2f6fc")
            d=ImageDraw.Draw(im)
            d.rounded_rectangle((85,85,395,275),radius=25,fill=color)
            d.rounded_rectangle((115,115,365,245),radius=12,fill="white")
            d.text((145,166),label,fill=color)
            out=io.BytesIO()
            im.save(out,format="JPEG")
            data=out.getvalue()
            aid=hashlib.sha256(data).hexdigest()
            await c.execute("INSERT INTO image_assets(asset_id,jpeg,width,height) VALUES($1,$2,480,360)",aid,data)

if __name__=="__main__":
    asyncio.run(main())
