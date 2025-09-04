import asyncio
import random

from sqlalchemy import exists, and_
from parsers import TelegramFetchComments
from models import Post, Comment, get_session


async def main(limit: int = 10):
    parser_comments = TelegramFetchComments()
    session = get_session()

    async with parser_comments.client:
        # Выбираем посты, по которым в БД еще нет комментариев
        query = session.query(Post).filter(~Post.comments.any())

        if limit is not None:
            query = query.limit(limit)

        posts_without_comments = query.all()

        print(f"Найдено {len(posts_without_comments)} постов без комментариев")

        # Пауза перед началом работы
        await asyncio.sleep(2)

        for index, post in enumerate(posts_without_comments):
            print(f"Скачиваем комментарии для поста {post.post_id} ({post.channel_name})")

            # Пауза между обработкой разных постов
            if index > 0:
                sleep_time = random.uniform(3, 10)
                print(f"Ждем {sleep_time:.2f} сек. перед следующим постом...")
                await asyncio.sleep(sleep_time)

            try:
                # Получаем комментарии
                comments = await parser_comments.fetch_comments_by_post(post.channel_name, post.post_id)

                # Дополнительная пауза на случай, если комментов было много
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Ошибка при получении комментариев для поста {post.post_id}: {e}")
                await asyncio.sleep(10)
                continue

            saved_count = 0
            for c in comments:
                # Проверка на существование комментария в БД
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
                saved_count += 1

            try:
                session.commit()
                print(f"Сохранено {saved_count} комментариев для поста {post.post_id}")
            except Exception as e:
                print(f"Ошибка при сохранении в БД для поста {post.post_id}: {e}")
                session.rollback()

        print("Обработка всех постов завершена.")


if __name__ == "__main__":
    asyncio.run(main(limit=100))
