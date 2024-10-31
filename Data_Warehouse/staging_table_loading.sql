SET search_path TO 'staging-tables';

CREATE TABLE region_data(
    code varchar(15),
    name varchar(100)
);

CREATE TABLE category_data(
    id text,
    category_type text,
    assignable varchar(25)
);

CREATE TABLE channel_data(
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
drop table video_data;
CREATE TABLE video_data (
    date_published timestamp,
    published_hours int,
    published_minutes int,
    region_code varchar(15),
    rank int,
    is_current int,
    category_id int,
    channel_id text,
    video_id text,
    view_count bigint,
    like_count bigint,
    comment_count int
);

COPY region_data
FROM 's3://youtube-data203/transformed_data/region_data/2024-10-27_17-43-08-379867/'
IAM_ROLE 'arn:aws:iam::010928224012:role/service-role/AmazonRedshift-CommandsAccessRole-20240928T201625'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY category_data
FROM 's3://youtube-data203/transformed_data/category_data/2024-10-27_17-43-25-812572/'
IAM_ROLE 'your_arns'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY channel_data
FROM 's3://youtube-data203/transformed_data/channel_data/2024-10-29/'
IAM_ROLE 'your_arns'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY video_data
FROM 's3://youtube-data203/transformed_data/videos_data/2024-10-29/'
IAM_ROLE 'your_arns'
FORMAT AS CSV
IGNOREHEADER 1
DELIMITER ',';

COPY video_data
FROM 's3://youtube-data203/transformed_data/videos_data/2024-10-31/'
IAM_ROLE 'your_arns'
DELIMITER ','
IGNOREHEADER 1
FORMAT AS CSV;
