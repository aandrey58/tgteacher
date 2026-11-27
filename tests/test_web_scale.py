"""Isolated PostgreSQL analytics checks and a reproducible concurrent web workload."""
import asyncio
import json
import math
import os
import sys
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bot"))
from tgteacher_bot import web_admin
from tgteacher_bot.db import user_repo, web_operations, srs_repo

ORIGIN="http://localhost:8080"
SECRET="ci-scale-only-"+"x"*32


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class WebScaleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn=os.environ["TGTEACHER_TEST_DSN"]
        schema="test_"+uuid.uuid4().hex
        admin=await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute,f'DROP SCHEMA "{schema}" CASCADE')
        self.pool=await asyncpg.create_pool(dsn,min_size=1,max_size=8,server_settings={"search_path":schema})
        self.addAsyncCleanup(self.pool.close)
        for mod in (user_repo,web_admin,web_operations,srs_repo):
            p=patch.object(mod,"get_pool",new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await user_repo.init_db_pg()
        async with self.pool.acquire() as c:
            await web_admin.init_web(c)
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=web_admin.build_app(SECRET,ORIGIN,initialize=False)),base_url=ORIGIN)
        self.addAsyncCleanup(self.client.aclose)
        response=await self.client.post("/api/login",json={"secret":SECRET},headers={"Origin":ORIGIN})
        self.assertEqual(response.status_code,200,response.text)

    async def get(self,path):
        response=await self.client.get("/api/"+path)
        self.assertEqual(response.status_code,200,(path,response.text))
        return response.json()

    async def action(self,name,args=None):
        response=await self.client.post("/api/action",json={"action":name,"args":args or {}},headers={"Origin":ORIGIN})
        self.assertEqual(response.status_code,200,response.text)
        return response.json()

    async def test_measured_analytics(self):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO users(user_id,username,registered_at,last_active_at,is_subscribed,subscription_until)
                VALUES(1,'free',now()-interval '2 days',now(),false,NULL),
                      (2,'vip',now()-interval '9 days',now()-interval '8 days',true,now()+interval '1 day'),
                      (3,'expired',now()-interval '9 days',now(),true,now()-interval '1 day');
                INSERT INTO families(id,name,target,archived_at) VALUES
                    (1,'Free group','FREE',NULL),(2,'VIP group','VIP',NULL),(3,'Archived group','FREE',now());
                INSERT INTO stage1_words(id,family_id,word_order,word,archived_at) VALUES
                    (1,1,0,'one',NULL),(2,2,0,'two',NULL),(3,3,0,'three',NULL),(4,1,1,'old',now());
                INSERT INTO srs_cards(user_id,family_id,task_id,due)
                    SELECT u.user_id,w.family_id,w.id,now()-interval '1 hour' FROM users u CROSS JOIN stage1_words w;
                UPDATE srs_cards SET due=now()+interval '1 day' WHERE user_id=2 AND task_id=2;
                INSERT INTO learner_answer_events(user_id,family_id,stage_num,task_id,answered_at,is_correct,source) VALUES
                    (1,1,2,1,now()-interval '1 hour',1,'lesson'),
                    (1,1,2,1,now()-interval '1 hour',0,'lesson'),
                    (2,1,2,1,now()-interval '8 days',1,'lesson'),
                    (1,1,2,1,now(),1,'legacy'),
                    (1,1,2,1,now()+interval '1 day',1,'lesson');
                INSERT INTO tts_requests(cache_key,provider,model,voice,status,reserved_microusd,reserved_at) VALUES
                    ('today','fake','m','v','failed',120,now()),
                    ('yesterday','fake','m','v','failed',999,now()-interval '1 day'),
                    ('future','fake','m','v','pending',999,now()+interval '1 day');
                INSERT INTO image_requests(cache_key,provider,model,size,status,reserved_microusd,reserved_at)
                    VALUES('today','fake','m','s','pending',100,now());
            """)
        dashboard=await self.get("dashboard")
        self.assertEqual(dashboard["metrics"]["published_groups"],2)
        self.assertEqual(dashboard["metrics"]["archived_groups"],1)
        self.assertEqual(dashboard["metrics"]["active_learners"],2)
        self.assertEqual(dashboard["metrics"]["due_cards"],3)
        self.assertEqual(sum(x["attempts"] for x in dashboard["activity"]),2)
        self.assertEqual(sum(x["learners"] for x in dashboard["activity"]),1)
        learners=await self.get("learners")
        self.assertEqual(learners["metrics"],{"total_learners":3,"active_learners":2,"vip_learners":1,"new_learners":1})
        self.assertEqual({r["user_id"]:r["due_cards"] for r in learners["items"]},{"1":1,"2":1,"3":1})
        reviews=await self.get("reviews")
        self.assertEqual(reviews["metrics"]["active_cards"],4)
        self.assertEqual(reviews["metrics"]["due_cards"],3)
        self.assertEqual(sum(r["due"] for r in reviews["items"]),3)
        self.assertEqual((await self.get("budgets"))["metrics"],{"TTS":120,"Images":100})
        for uid in (1,2,3):
            self.assertEqual((await srs_repo.learning_statistics(uid))["due_cards"],1)

    async def test_concurrent_dashboard_scale(self):
        # A repeatable synthetic workload, never run against production data.
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO users(user_id,username,registered_at,last_active_at,is_subscribed,subscription_until)
                    SELECT i,'synthetic-'||i,now()-interval '14 days',
                        now()-((i%10)::text||' days')::interval,(i%4=0),
                        CASE WHEN i%4=0 THEN now()+interval '30 days' ELSE NULL END
                    FROM generate_series(1,5000) i;
                INSERT INTO families(id,name,target)
                    SELECT i,'Synthetic group '||i,'FREE' FROM generate_series(1,100) i;
                INSERT INTO stage1_words(family_id,word_order,word)
                    SELECT f.id,w,'word-'||w FROM families f CROSS JOIN generate_series(1,20) w;
                INSERT INTO user_family_progress(user_id,family_idx)
                    SELECT user_id,((user_id-1)%100+1)::integer FROM users;
                INSERT INTO srs_cards(user_id,family_id,task_id,due)
                    SELECT u.user_id,w.family_id,w.id,now()-interval '1 hour'
                    FROM users u JOIN stage1_words w ON w.family_id=((u.user_id-1)%100+1);
                INSERT INTO learner_answer_events(user_id,family_id,stage_num,task_id,answered_at,is_correct,source)
                    SELECT user_id,family_id,2,task_id,now()-interval '1 hour',1,'lesson' FROM srs_cards;
                INSERT INTO web_campaigns(id,target,body,status)
                    SELECT md5(i::text)::uuid,'selected','synthetic campaign '||i,'draft' FROM generate_series(1,100) i;
                INSERT INTO web_deliveries(campaign_id,user_id)
                    SELECT w.id,u.user_id FROM web_campaigns w CROSS JOIN users u WHERE u.user_id<=1000;
                INSERT INTO web_support(user_id,question) SELECT user_id,'synthetic question' FROM users;
            """)
            for table in ("users","families","stage1_words","srs_cards","learner_answer_events","user_family_progress","web_campaigns","web_deliveries","web_support"):
                await c.execute("ANALYZE "+table)
        self.assertEqual((await self.get("dashboard"))["metrics"]["due_cards"],100000)
        self.assertEqual((await self.get("learners"))["metrics"]["total_learners"],5000)
        self.assertEqual((await self.get("reviews"))["metrics"]["due_cards"],100000)
        first=await self.get("groups")
        second=await self.get("groups?offset=50")
        self.assertTrue(first["next"])
        self.assertFalse(second["next"])
        self.assertEqual(len({r["id"] for r in first["items"]+second["items"]}),100)
        self.assertTrue(all(r["learners"]==50 and r["words"]==20 for r in first["items"]+second["items"]))
        campaigns=await self.action("campaigns")
        self.assertEqual(len(campaigns["items"]),50)
        self.assertTrue(all(r["recipients"]==1000 for r in campaigns["items"]))
        endpoints=("dashboard","groups","groups?sort=words","learners","learners?offset=1000","reviews","campaigns","support")
        times={path:[] for path in endpoints}
        sem=asyncio.Semaphore(8)
        async def request(path):
            async with sem:
                before=time.perf_counter()
                if path in ("campaigns","support"):
                    await self.action(path)
                else:
                    await self.get(path)
                times[path].append((time.perf_counter()-before)*1000)
        await asyncio.gather(*(request(path) for _ in range(10) for path in endpoints))
        result={path:{"requests":len(ms),"p95_ms":round(sorted(ms)[math.ceil(len(ms)*.95)-1],1),"max_ms":round(max(ms),1)} for path,ms in times.items()}
        print("WEB_SCALE_RESULT "+json.dumps({"users":5000,"groups":100,"cards":100000,"answer_events":100000,"deliveries":100000,"concurrency":8,"endpoints":result},sort_keys=True),flush=True)
        for path,stats in result.items():
            self.assertLess(stats["p95_ms"],3000,(path,stats))
