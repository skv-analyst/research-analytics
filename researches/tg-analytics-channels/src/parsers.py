from telethon import TelegramClient
from config import PATH_TO_SESSION, TG_USER_APP_API_ID, TG_USER_APP_API_HASH


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


class TelegramFetchComments:
    def __init__(self):
        session_path = PATH_TO_SESSION / "my_session"
        self.client = TelegramClient(str(session_path), TG_USER_APP_API_ID, TG_USER_APP_API_HASH)

    async def fetch_comments_by_post(self, channel: str, post_id: int):
        """
        Получает комментарии к конкретному посту по его ID.
        """
        results = []

        async with self.client:
            post = await self.client.get_messages(channel, ids=post_id)

            if not post:
                print(f"Пост {post_id} не найден в канале {channel}")
                return results

            async for reply in self.client.iter_messages(channel, reply_to=post_id):
                author = await reply.get_sender()
                if not author:
                    continue

                results.append({
                    "channel_id": post.peer_id.channel_id if hasattr(post.peer_id, "channel_id") else None,
                    "post_id": post.id,
                    "comment_id": reply.id,
                    "comment_date": reply.date,
                    "author_id": getattr(author, "id", None),
                    "author_title": getattr(author, "title", None),
                    "author_username": getattr(author, "username", None),
                    "author_first_name": getattr(author, "first_name", None),
                    "author_last_name": getattr(author, "last_name", None)
                })

        return results
