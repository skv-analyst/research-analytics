from sqlalchemy import (
    Column, Integer, String, DateTime, create_engine,
    UniqueConstraint, ForeignKeyConstraint, and_
)
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

    # Связь с комментариями
    comments = relationship(
        "Comment",
        back_populates="post",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        # уникальность поста определяется комбинацией channel_id + post_id
        UniqueConstraint("channel_id", "post_id", name="uix_channel_post"),
    )


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, nullable=False)
    post_id = Column(Integer, nullable=False)
    comment_id = Column(Integer, nullable=False)
    comment_date = Column(DateTime, nullable=False)
    author_uuid = Column(Integer, nullable=True)
    author_title = Column(String, nullable=True)

    # Связь с Post через составной ключ (channel_id + post_id)
    post = relationship(
        "Post",
        back_populates="comments",
        primaryjoin=and_(channel_id == Post.channel_id, post_id == Post.post_id)
    )

    __table_args__ = (
        # уникальность комментария определяется (канал + пост + comment_id)
        UniqueConstraint("channel_id", "post_id", "comment_id", name="uix_channel_post_comment"),
        # составной ForeignKey к Post
        ForeignKeyConstraint(
            ['channel_id', 'post_id'],
            ['posts.channel_id', 'posts.post_id']
        ),
    )


def get_session(db_path="sqlite:///tg_analytics_channels.db"):
    engine = create_engine(db_path, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()