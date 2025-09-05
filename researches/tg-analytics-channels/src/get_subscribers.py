import asyncio
from models import get_session, Subscribers, Post
from parsers import TelegramFetchSubscribers


async def main():
    session = get_session()
    parser = TelegramFetchSubscribers()

    channels = session.query(Post.channel_id, Post.channel_name).distinct().limit(3).all()

    async with parser.client:
        for channel_id, channel_name in channels:
            result = await parser.get_subscriber_count(channel_name)

            snapshot = Subscribers(
                channel_id=result["channel_id"],
                channel_name=result["channel_name"],
                subscribers=result["subscribers"],
                timestamp=result["timestamp"],
            )

            session.add(snapshot)

        session.commit()
        print(f"Сохранено {len(channels)} записей")


if __name__ == "__main__":
    asyncio.run(main())
