CREATE DATABASE IF NOT EXISTS stage_db;

    CREATE TABLE IF NOT EXISTS stage_db.users(
        user_id UInt32,
        birth_date DateTime64,
        gender Enum8('male', 'female'),
		reg_date Date,
        update_ts DateTime,
        insertion_ts DateTime
    )
    ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
    ORDER BY (insertion_ts);
    
    
    CREATE TABLE IF NOT EXISTS stage_db.orders(
        order_id UInt32,
        creation_time DateTime,
        insertion_ts DateTime
    )
    ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
    ORDER BY (insertion_ts);
    
    CREATE TABLE IF NOT EXISTS stage_db.order_items(
    	order_id UInt32,
    	product_id UInt8,
        product_quant UInt8,
        creation_time DateTime,
        insertion_ts DateTime
    )
    ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
    ORDER BY (insertion_ts);


    CREATE TABLE IF NOT EXISTS stage_db.user_actions(
        user_id UInt32,
        order_id UInt32,
        user_action Enum8('create_order', 'cancel_order'),
        action_time DateTime,
        insertion_ts DateTime
    )
    ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
    ORDER BY (insertion_ts);


    CREATE TABLE IF NOT EXISTS stage_db.products(
        product_id UInt16,
        name LowCardinality(String),
        price UInt32,
        update_ts DateTime,
        insertion_ts DateTime
    )
    ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
    ORDER BY (insertion_ts);


	SET flatten_nested = 0;
	CREATE TABLE IF NOT EXISTS stage_db.sellers(
		seller_info Nested (
			`seller_id` UInt16,
			`status` Enum8('basic', 'premium'),
			`risk_score` DECIMAL32(4)
		),
		insertion_ts DateTime
	)
	ENGINE = MergeTree
	PARTITION BY toYYYYMMDD(insertion_ts)
	ORDER BY (insertion_ts);