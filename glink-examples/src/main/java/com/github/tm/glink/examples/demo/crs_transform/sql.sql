CREATE TABLE csv_table (
    pid INT,
    carno STRING,
    lat DOUBLE,
    lng DOUBLE,
    speed DOUBLE,
    azimuth INT,
    `timestamp` BIGINT,
    status INT
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///home/liebing/input/par/1',
    'format' = 'csv'
)

CREATE TABLE Points_sink (
    pid INT,
    carno STRING,
    wkt STRING,
    speed DOUBLE,
    azimuth INT,
    `timestamp` BIGINT,
    status INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'Points_sink',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'avro',
    'scan.startup.mode' = 'earliest-offset'
)