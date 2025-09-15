/*  Разметка каналов    */
update channels
set is_target = false
where channel_id in (
    '2210583022', '1920871407', '2261193305', '2623174751', '1612936900',
    '1471400937', '1927207017', '1846459118', '1249488807', '1958547069',
    '2554598563', '1625297163', '2396618578', '2070115493', '2865153300',
    '2000845863', '1707750488', '2129026250', '1521803616', '1320517804',
    '2119256117', '1888357186', '1712153129', '1510889166', '2683934139',
    '2423056588', '2376970010','1106133409'
    );

update channels
set is_target = true
where is_target is null;


/*  Обзор   */
with
    target_channels as (
        select
            channel_id,
            channel_name,
            subscribers,
            timestamp
        from channels
        where is_target is true
    ),

    target_posts as (
        select
            channel_id,
            channel_name,
            channel_title,
            count(post_id) as grab_post_cnt
        from posts
        where post_replies > 0
        group by channel_id, channel_name, channel_title
    ),

    target_comments as (
        select
            channel_id,
            channel_name,
            count(distinct post_id) as save_post_cnt,
            count(*) as save_comment_cnt,
            count(*) filter (where author_username = 'user') as save_comment_user,
            count(*) filter (where author_username != 'user') as save_comment_channel,
            count(*) filter (where author_username != 'user' and channel_name != author_username ) as save_comment_channel_external

        from (
            select
                channel.channel_id,
                channel.channel_name,
                comment.post_id,
                comment.comment_id,
                comment.author_username
            from comments as comment
            left join channels as channel on comment.channel_id = channel.channel_id)
        group by channel_id, channel_name
    ),

    results as (
        select
            channel.channel_id,
            channel.subscribers,
            channel.channel_name,
            tp.channel_title,
            tp.grab_post_cnt,
            tc.save_post_cnt,
            tc.save_comment_cnt,
            tc.save_comment_user,
            tc.save_comment_channel,
            tc.save_comment_channel_external

        from target_channels as channel
        left join target_posts as tp on channel.channel_id=tp.channel_id
        left join target_comments as tc on channel.channel_id = tc.channel_id
    )

-- select * from target_channels
-- select * from target_posts
-- select * from target_comments
-- select * from results
--
select
    count(distinct channel_id) as channel_cnt,
    sum(grab_post_cnt) as grab_post_cnt,
    sum(save_post_cnt) as save_post_cnt,
    sum(save_comment_cnt) as save_comment_cnt
from results;