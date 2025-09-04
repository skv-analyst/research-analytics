import asyncio
import time

from sqlalchemy import exists, and_
from tg_parser import TelegramFetchPosts
from models import Post, get_session


async def main():
    parser_posts = TelegramFetchPosts()
    channels = ["tagir_analyzes", "zarplatnik_analytics"]

    async with parser_posts.client:
        session = get_session()

        for channel in channels:
            channel_entity = await parser_posts.get_channel_information(channel)
            posts = await parser_posts.get_channel_posts(channel_entity, post_counts=100)

            for p in posts:
                # проверяем, есть ли пост в БД
                exists_query = session.query(
                    exists().where(
                        and_(Post.channel_id == p["channel_id"], Post.post_id == p["post_id"])
                    )
                ).scalar()

                # пропускаем, если есть
                if exists_query:
                    continue

                # сохраняем если нет
                post = Post(**p)
                session.add(post)

            session.commit()
            print(f"{channel}: посты собраны")
            time.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
