"""Server operator CLI. No create/refund calls; dry-run unless --apply is explicit.
Run inside the bot container: python -m scripts.reconcile_payments ...
"""
import argparse
import asyncio
import json
from tgteacher_bot.db import payment_orders as orders, user_repo
from tgteacher_bot.services.payments.yookassa_payment import YooKassaPayment

async def run(args):
    await user_repo.init_db_pg()
    if args.command=="list":
        result=await orders.operator_orders()
    elif args.command=="legacy":
        result=await YooKassaPayment().inspect_legacy(args.provider_id)
    else:
        result=await YooKassaPayment().reconcile(args.user_id,args.order_id,args.provider_id,apply=args.apply)
    # Never output credentials, payer email, payment URL or raw SDK payload.
    if isinstance(result,dict):
        result.pop("payment_url",None)
    print(json.dumps(result,ensure_ascii=False,default=str,indent=2))

def main():
    p=argparse.ArgumentParser(description=__doc__)
    commands=p.add_subparsers(dest="command",required=True)
    commands.add_parser("list")
    legacy=commands.add_parser("legacy")
    legacy.add_argument("provider_id")
    reconcile=commands.add_parser("reconcile")
    reconcile.add_argument("user_id",type=int)
    reconcile.add_argument("order_id")
    reconcile.add_argument("provider_id")
    reconcile.add_argument("--apply",action="store_true")
    asyncio.run(run(p.parse_args()))

if __name__=="__main__":
    main()
