import asyncio
from models import get_session, Channels, Post
from parsers import TelegramFetchSubscribers


async def main():
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
