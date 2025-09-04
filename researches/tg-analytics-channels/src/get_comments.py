import asyncio
import time

from sqlalchemy import exists, and_
from parsers import TelegramFetchComments
from models import Post, Comment, get_session


async def main(limit: int = 10):
    parser_comments = TelegramFetchComments()
    session = get_session()

    async with parser_comments.client:
        # выбираем посты, по которым в БД еще нет комментариев
        query = session.query(Post).filter(~Post.comments.any())

        if limit is not None:
            query = query.limit(limit)

        posts_without_comments = query.all()

        print(f"Найдено {len(posts_without_comments)} постов без комментариев")

        for post in posts_without_comments:
            print(f"🔍 Скачиваем комментарии для поста {post.post_id} ({post.channel_name})")

            comments = await parser_comments.fetch_comments_by_post(post.channel_name, post.post_id)

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
                    author_id=c["author_id"],
                    author_title=c["author_title"],
                    author_username=c["author_username"],
                    author_first_name=c["author_first_name"],
                    author_last_name=c["author_last_name"]
                )
                session.add(comment)

            session.commit()
            print(f"Сохранено {len(comments)} комментариев для поста {post.post_id}")
            time.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
