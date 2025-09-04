from sqlalchemy import Column, Integer, String, DateTime, create_engine, UniqueConstraint, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

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

    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", name="uix_channel_post"),
    )


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, nullable=False)
    post_id = Column(Integer, ForeignKey("posts.post_id"), nullable=False)  # связь с Post
    comment_id = Column(Integer, nullable=False)
    comment_date = Column(DateTime, nullable=False)
    author_id = Column(Integer, nullable=True)
    author_title = Column(Integer, nullable=True)
    author_username = Column(String, nullable=True)
    author_first_name = Column(String, nullable=True)
    author_last_name = Column(String, nullable=True)

    post = relationship("Post", back_populates="comments")

    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", "comment_id", name="uix_channel_post_comment"),
    )


def get_session(db_path="sqlite:///tg_analytics_channels.db"):
    engine = create_engine(db_path, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
