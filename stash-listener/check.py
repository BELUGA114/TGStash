import os, asyncio
from pyrogram import Client

async def main():
    app = Client('listener', api_id=int(os.environ['TG_API_ID']),
                 api_hash=os.environ['TG_API_HASH'], workdir='/data/session')
    async with app:
        chat_id = int(os.environ['RECEIVE_CHAT_ID'])
        count = 0
        async for msg in app.get_chat_history(chat_id, min_id=41, reverse=True):
            count += 1
            preview = str(msg.media or msg.text or '(empty)')
            print(f'{msg.id}: {preview[:60]}')
        print(f'checkpoint 之后共 {count} 条消息')
asyncio.run(main())
