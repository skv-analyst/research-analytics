"""
СКРИПТ СБОРА КОММЕНТАРИЕВ
Сбор комментариев к постам из целевых каналов.
Задача: Обновляет комментарии для постов, где их недостаточно.
Особенности: Работает только с целевыми каналами, избегает дубликатов.
"""

import asyncio
import random
import time
import uuid
from tqdm.asyncio import tqdm_asyncio

from sqlalchemy import exists, and_, func
from parsers import TelegramFetchComments
from models import Post, Comment, Channels, get_session


def hash_author_id_uuid(author_id: int) -> str:
    """Генерация UUID на основе ID автора для анонимизации"""

    if author_id is None:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(author_id)))


async def main(limit: int = 10):
    """Основная функция: сбор комментариев для постов целевых каналов"""

    parser_comments = TelegramFetchComments()
    session = get_session()

    async with parser_comments.client:
        # Количество уже собранных комментариев по каждому посту
        comment_counts = (
            session.query(
                Comment.channel_id,
                Comment.post_id,
                func.count(Comment.comment_id).label("saved_count")
            )
            .group_by(Comment.channel_id, Comment.post_id)
            .subquery()
        )

        # Выбираем посты только из целевых каналов, у которых комментариев меньше, чем post_replies - 10
        query = (
            session.query(Post)
            .join(Channels, Channels.channel_id == Post.channel_id)
            .outerjoin(
                comment_counts,
                (comment_counts.c.channel_id == Post.channel_id) &
                (comment_counts.c.post_id == Post.post_id)
            )
            .filter(
                Channels.is_target.is_(True),
                Post.post_replies > 0,
                func.coalesce(comment_counts.c.saved_count, 0) < (Post.post_replies - 10)
            )
        )

        if limit is not None:
            query = query.limit(limit)
        posts_without_comments = query.all()

        for post in tqdm_asyncio(posts_without_comments, desc="Обработка постов", unit="post"):
            await asyncio.sleep(random.uniform(1, 5))

            try:
                # Получаем комментарии
                comments = await parser_comments.fetch_comments_by_post(post.channel_name, post.post_id)
                await asyncio.sleep(0.5)
            except Exception:
                await asyncio.sleep(10)
                continue

            for c in comments:
                exists_query = session.query(
                    exists().where(
                        and_(
                            Comment.channel_id == c["channel_id"],
                            Comment.post_id == c["post_id"],
                            Comment.comment_id == c["comment_id"],
                        )
                    )
                ).scalar()

                if exists_query:
                    continue

                comment = Comment(
                    channel_id=c["channel_id"],
                    post_id=c["post_id"],
                    comment_id=c["comment_id"],
                    comment_date=c["comment_date"],
                    author_uuid=hash_author_id_uuid(c["author_id"]),
                    author_title=c["author_title"] if c["author_title"] else "user",
                    author_username=c["author_username"] if c["author_title"] else "user",
                )
                session.add(comment)

            try:
                session.commit()
            except Exception:
                session.rollback()


if __name__ == "__main__":
    for i in range(1, 11):
        time.sleep(5)
        asyncio.run(main(100))