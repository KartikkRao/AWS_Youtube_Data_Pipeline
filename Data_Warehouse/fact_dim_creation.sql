SET search_path TO 'fact_dim_tables';

CREATE TABLE fact_dim_tables.dim_region(
    code varchar(15),
    name varchar(100)
);

CREATE TABLE dim_category(
    category_type text,
    assignable varchar(15),
    category_id text
);

CREATE TABLE dim_video_date(
    date_id bigint IDENTITY(1,1),
    date_published date,
    year int,
    month int,
    week int,
    day int,
    hours int,
    minutes int
);

CREATE TABLE dim_channel(
    channel_id text,
    channel_name text,
    date_created date,
    country_of_origin varchar(50),
    view_count bigint,
    subcriber_count bigint,
    video_count int,
    start_date date,
    end_date date,
    is_current int
);

CREATE TABLE fact_video(
    video_id text,
    category_id int,
    date_id bigint,
    channel_id text,
    region_code text,
    view_count bigint,
    like_count bigint,
    comment_count bigint,
    rank int,
    is_current int
);
