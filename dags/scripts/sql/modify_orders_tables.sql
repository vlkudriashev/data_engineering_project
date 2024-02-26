-- создаём новую таблицу для наименований в заказе
CREATE TABLE IF NOT EXISTS source.order_items(
	order_id integer,
	product_id smallint,
	product_quant smallint,
	creation_time timestamp
);


-- заполняем таблицу order_items
INSERT INTO source.order_items
SELECT 
	order_id,
	unnest(product_ids) as product_id,
	unnest(product_quant) as product_quant,
	creation_time
FROM source.orders;

-- удаляем более не нужные колонки, которые больше не нужны
-- из таблицы orders
ALTER TABLE source.orders
DROP COLUMN product_ids, DROP COLUMN product_quant;

-- добавляем внешние ключи для вновь созданной таблицы
ALTER TABLE source.order_items
ADD CONSTRAINT order_items_order_fk
FOREIGN KEY (order_id) 
REFERENCES source.orders (order_id)
ON UPDATE CASCADE;

ALTER TABLE source.order_items
ADD CONSTRAINT order_items_product_fk
FOREIGN KEY (product_id) 
REFERENCES source.products (product_id)
ON UPDATE CASCADE;
