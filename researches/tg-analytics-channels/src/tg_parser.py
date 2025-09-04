from telethon import TelegramClient
from config import PATH_TO_SESSION, TG_USER_APP_API_ID, TG_USER_APP_API_HASH
from datetime import datetime


class TelegramFetchPosts:
    def __init__(self):
        session_path = PATH_TO_SESSION / "my_session"
        self.client = TelegramClient(str(session_path), TG_USER_APP_API_ID, TG_USER_APP_API_HASH)

    async def get_channel_information(self, channel_name: str):
        return await self.client.get_entity(channel_name)

    async def get_channel_posts(self, channel_entity, post_counts: int = 1):
        posts = await self.client.get_messages(channel_entity, limit=post_counts)

        results = []
        for post in posts:
            if not post.text or not post.text.strip():
                continue

            post_data = {
                "channel_id": channel_entity.id,
                "channel_name": channel_entity.username,
                "channel_title": channel_entity.title,
                "post_id": post.id,
                "post_date": post.date,
                "post_replies": post.replies.replies if post.replies else 0,
                "post_views": getattr(post, "views", 0),
                "post_forwards": getattr(post, "forwards", 0),
                "post_preview": (post.text[:50] + "...") if post.text and len(post.text) > 50 else post.text,
            }
            results.append(post_data)

        return results




# from pprint import pprint
#
# from datetime import datetime
# from telethon import TelegramClient
# from config import PATH_TO_SESSION, TG_USER_APP_API_ID, TG_USER_APP_API_HASH
#
# session_path = PATH_TO_SESSION / "my_session"
#
# client = TelegramClient(str(session_path), TG_USER_APP_API_ID, TG_USER_APP_API_HASH)
#
#
# async def get_channel_information(channel_name: str = None):
#     entity = await client.get_entity(channel_name)
#     return entity
#
#
# async def get_channel_posts(channel_entity, post_counts: int = 5):
#     posts = await client.get_messages(channel_entity, limit=post_counts)
#
#     results = []
#
#     for post in posts:
#         # Пропускаем посты с пустым текстом
#         if not post.text or not post.text.strip():
#             continue
#
#         post_data = {
#             "channel_id": channel_entity.id,
#             "channel_name": channel_entity.username,
#             "channel_title": channel_entity.title,
#             'post_id': post.id,
#             'post_date': post.date.isoformat(),
#             'post_replies': post.replies.replies if post.replies else 0,
#             'post_views': getattr(post, 'views', 0),
#             'post_forwards': getattr(post, 'forwards', 0),
#             'post_preview': (post.text[:50] + '...') if post.text and len(post.text) > 50 else post.text
#         }
#
#         results.append(post_data)
#
#     return results
#
#
# async def main():
#     channels = ["zarplatnik_analytics"]
#     for channel in channels:
#         channel_entity = await get_channel_information(channel)
#         posts = await get_channel_posts(channel_entity, post_counts=10)
#
#         pprint(posts)
#
# if __name__ == "__main__":
#     with client:
#         client.loop.run_until_complete(main())
#
