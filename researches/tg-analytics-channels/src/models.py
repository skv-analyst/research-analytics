from sqlalchemy import Column, Integer, String, DateTime, create_engine, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, nullable=False)
    channel_name = Column(String, nullable=True)
    channel_title = Column(String, nullable=True)
    post_id = Column(Integer, nullable=False)
    post_date = Column(DateTime, nullable=False)
    post_replies = Column(Integer, default=0)
    post_views = Column(Integer, default=0)
    post_forwards = Column(Integer, default=0)
    post_preview = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", name="uix_channel_post"),
    )


def get_session(db_path="sqlite:///tg-analytics-channels.db"):
    engine = create_engine(db_path, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
