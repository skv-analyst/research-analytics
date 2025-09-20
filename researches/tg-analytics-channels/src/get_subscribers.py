"""
СКРИПТ СБОРА СТАТИСТИКИ ПОДПИСЧИКОВ
Периодический сбор статистики по подписчикам каналов.
Задача: Обновляет количество подписчиков для всех каналов в БД.
"""

import asyncio
from models import get_session, Channels, Post
from parsers import TelegramFetchSubscribers


async def main():
    """Основная функция: собирает статистику подписчиков для всех каналов"""

    session = get_session()
    parser = TelegramFetchSubscribers()

    channels = session.query(Post.channel_id, Post.channel_name).distinct().all()

    async with parser.client:
        for channel_id, channel_name in channels:
            await asyncio.sleep(1)
            result = await parser.get_subscriber_count(channel_name)

            snapshot = Channels(
                channel_id=result["channel_id"],
                channel_name=result["channel_name"],
                subscribers=result["subscribers"],
                timestamp=result["timestamp"]
            )

            session.add(snapshot)

        session.commit()
        print(f"Сохранено {len(channels)} записей")


if __name__ == "__main__":
    asyncio.run(main())
