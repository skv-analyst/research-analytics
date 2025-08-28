from telethon import TelegramClient
from config import PATH_TO_SESSION, TG_USER_APP_API_ID, TG_USER_APP_API_HASH

session_path = PATH_TO_SESSION / "my_session"

client = TelegramClient(str(session_path), TG_USER_APP_API_ID, TG_USER_APP_API_HASH)

async def main():
    channel = "select_all_from_analytics"

    # берём последние 10 постов
    async for message in client.iter_messages(channel, limit=10):
        if message.replies:
            print(f"Пост {message.id} имеет {message.replies.replies} комментариев")

            # достаём комментарии к этому посту
            async for reply in client.iter_messages(channel, reply_to=message.id):
                author = await reply.get_sender()
                print("  Комментарий от:", author.username or author.id)

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())