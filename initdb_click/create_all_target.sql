CREATE DATABASE IF NOT EXISTS target_db;
    
    CREATE TABLE IF NOT EXISTS target_db.users(
        user_id UInt32,
        birth_date DateTime64,
        gender Enum8('male', 'female'),
		reg_date Date,
        update_ts DateTime,
        insertion_ts DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (user_id, reg_date);


    CREATE TABLE IF NOT EXISTS target_db.orders(
        order_id UInt32,
        creation_time DateTime,
        product_ids Array(UInt16),
        product_quant Array(UInt16),
        insertion_ts DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (order_id, creation_time);


    CREATE TABLE IF NOT EXISTS target_db.user_actions(
        user_id UInt32,
        order_id UInt32,
        user_action Enum8('create_order', 'cancel_order'),
        action_time DateTime,
        insertion_ts DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (order_id, action_time);


    CREATE TABLE IF NOT EXISTS target_db.products(
        product_id UInt16,
        name LowCardinality(String),
        price UInt32,
		record_start DateTime,
		record_end DateTime,
        insertion_ts DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (product_id, record_start);


    CREATE TABLE IF NOT EXISTS target_db.sellers(
        seller_id UInt16,
        seller_status Enum8('basic', 'premium'),
        risk_score Decimal,
        insertion_ts DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (seller_id, insertion_ts);

	
	
