-- SCD querry for channel_data
BEGIN;

CREATE TEMP TABLE new_channel_rows AS(
    SELECT
        c.channel_id,
        c.channel_name,
        c.date_created,
        c.country_of_origin,
        c.view_count,
        c.subcriber_count,
        c.video_count,
        c.start_date,
        c.end_date,
        c.is_current,
    CASE
        WHEN 
            c.channel_id = d.channel_id AND d.is_current = 1 
            AND(c.view_count <> d.view_count OR
                c.subcriber_count <> d.subcriber_count OR
                c.video_count <> d.video_count)
        THEN 1
        WHEN d.channel_id IS NULL THEN 2
        ELSE 0
    END AS action
    FROM
        "staging-tables".channel_data AS c
    LEFT JOIN
        "fact_dim_tables".dim_channel AS d
    on
        c.channel_id = d.channel_id
);

UPDATE "fact_dim_tables".dim_channel
SET 
    is_current = 0, 
    end_date = new_channel_rows.start_date
FROM 
    new_channel_rows 
WHERE
    new_channel_rows.channel_id = "fact_dim_tables".dim_channel.channel_id AND
    new_channel_rows.action = 1;

INSERT INTO "fact_dim_tables".dim_channel (channel_id, channel_name, date_created, country_of_origin, view_count, subcriber_count, video_count, start_date, end_date, is_current )
SELECT 
    channel_id,
    channel_name,
    date_created,
    country_of_origin,
    view_count,
    subcriber_count,
    video_count,
    start_date,
    end_date,
    is_current
FROM 
    new_channel_rows
WHERE
    action in (1,2);

COMMIT;

--TESTING
--DROP TABLE IF EXISTS new_channel_rows;
--select count(*) from "fact_dim_tables".dim_channel;
--select * from "fact_dim_tables".dim_channel where channel_id = 'UCkaMZOILqfN5EYmQM0MmkIQ';

BEGIN;

CREATE TEMP TABLE new_date_rows AS(
    SELECT DISTINCT
        d.date_id,
        v.date_published,
        EXTRACT(Year FROM v.date_published) AS year,
        EXTRACT(MONTH FROM v.date_published) AS month,
        EXTRACT(WEEK FROM v.date_published) AS week,
        EXTRACT(DAY FROM v.date_published) AS day,
        v.published_hours,
        v.published_minutes,
        CASE
            WHEN d.date_id IS NULL THEN 1
            ELSE 0
        END AS action
    FROM 
        "staging-tables".video_data AS v 
    LEFT JOIN
        "fact_dim_tables".dim_video_date AS d 
    ON
        v.date_published = d.date_published AND 
        v.published_hours = d.hours AND 
        v.published_minutes = d.minutes 
);

INSERT INTO "fact_dim_tables".dim_video_date(date_published,year,month,week,day,hours,minutes)
SELECT 
    n.date_published,
    n.year,
    n.month,
    n.week,
    n.day,
    n.published_hours,
    n.published_minutes
FROM new_date_rows AS n 
WHERE n.action = 1;

COMMIT;

--TESTING
--DROP TABLE IF EXISTS new_date_rows;
--select count(*) from "staging-tables".video_data;
--select count(*) from "fact_dim_tables".dim_video_date;

BEGIN;
CREATE TEMP TABLE fact_new_rows AS(
    SELECT
        v.video_id,
        v.category_id,
        d.date_id,
        v.channel_id,
        v.region_code,
        v.view_count,
        v.like_count,
        v.comment_count,
        v.rank,
        v.is_current,
        CASE 
            WHEN (fv1.video_id IS NULL ) THEN 1
            ELSE 0
        END AS action 
    FROM 
        "staging-tables".video_data AS v 
    LEFT JOIN "fact_dim_tables".dim_video_date AS d
    ON 
        d.date_published = v.date_published and 
        d.hours = v.published_hours and 
        d.minutes = v.published_minutes
    LEFT JOIN "fact_dim_tables".fact_video AS fv1
    ON 
        v.video_id = fv1.video_id AND 
        v.category_id = fv1.category_id AND 
        d.date_id = fv1.date_id AND
        v.channel_id = fv1.channel_id AND 
        v.region_code = fv1.region_code AND
        v.rank = fv1.rank
);

UPDATE "fact_dim_tables".fact_video 
SET 
    is_current = 0
FROM 
    fact_new_rows as f
WHERE
    f.region_code = "fact_dim_tables".fact_video.region_code AND
    f.rank = "fact_dim_tables".fact_video.rank AND
    f.video_id <> "fact_dim_tables".fact_video.video_id;

INSERT INTO "fact_dim_tables".fact_video (video_id, category_id, date_id, channel_id, region_code, view_count, like_count, comment_count, rank, is_current)
SELECT 
    video_id, 
    category_id, 
    date_id, 
    channel_id, 
    region_code, 
    view_count, 
    like_count, 
    comment_count, 
    rank, 
    is_current
FROM 
    fact_new_rows as f
WHERE 
    f.action = 1;

COMMIT;


--DROP TABLE IF EXISTS fact_new_rows;
-- select * from "fact_dim_tables".fact_video;
