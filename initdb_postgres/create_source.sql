CREATE SCHEMA IF NOT EXISTS source;

    CREATE TABLE IF NOT EXISTS source.users(
        user_id integer PRIMARY KEY,
        birth_date Date,
        gender varchar(6),
        reg_date Date,
        update_ts timestamp
    );


    CREATE TABLE IF NOT EXISTS source.orders(
        order_id bigint PRIMARY KEY,
        creation_time timestamp,
        product_ids smallint[],
        product_quant smallint[]
    );


    CREATE TABLE IF NOT EXISTS source.user_actions(
        user_id integer,
        order_id bigint,
        action varchar(20),
        action_time timestamp
    );


    CREATE TABLE IF NOT EXISTS source.products(
        product_id smallint PRIMARY KEY,
        name varchar(50),
        price integer,
        update_ts timestamp
    );
    

-- Добавляем ограничения для внешних ключей

ALTER TABLE source.user_actions
ADD CONSTRAINT user_actions_user_fk
FOREIGN KEY (user_id) 
REFERENCES source.users (user_id)
ON UPDATE CASCADE;

ALTER TABLE source.user_actions
ADD CONSTRAINT user_actions_order_fk
FOREIGN KEY (user_id) 
REFERENCES source.users (user_id)
ON UPDATE CASCADE;
