import asyncio
import random
import uuid
from tqdm.asyncio import tqdm_asyncio

from sqlalchemy import exists, and_
from parsers import TelegramFetchComments
from models import Post, Comment, get_session


def hash_author_id_uuid(author_id: int) -> str:
    if author_id is None:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(author_id)))


async def main(limit: int = 10):
    parser_comments = TelegramFetchComments()
    session = get_session()

    async with parser_comments.client:
        # Выбираем посты, по которым в БД еще нет комментариев
        query = session.query(Post).filter(~Post.comments.any())
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
    asyncio.run(main(100))
