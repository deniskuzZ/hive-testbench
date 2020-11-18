create database if not exists ${DB_DELTA};
use ${DB_DELTA};

DROP TABLE IF EXISTS s_store_returns;

CREATE EXTERNAL TABLE s_store_returns (
    sret_store_id char(16),
    sret_purchase_id char(16),
    sret_line_number int,
    sret_item_id char(16),
    sret_customer_id char(16),
    sret_return_date char(10),
    sret_return_time char(10),
    sret_ticket_number char(20),
    sret_return_qty int,
    sret_return_amount float,
    sret_return_tax float,
    sret_return_fee float,
    sret_return_ship_cost float,
    sret_refunded_cash float,
    sret_reversed_charge float,
    sret_store_credit float,
    sret_reason_id char(16))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_store_returns';


DROP TABLE IF EXISTS s_catalog_order;

CREATE EXTERNAL TABLE s_catalog_order (
    cord_order_id int,
    cord_bill_customer_id char(16),
    cord_ship_customer_id char(16),
    cord_order_date char(10),
    cord_order_time int,
    cord_ship_mode_id char(16),
    cord_call_center_id char(16),
    cord_order_comments varchar(100))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_catalog_order';


DROP TABLE IF EXISTS s_catalog_order_lineitem;

CREATE EXTERNAL TABLE s_catalog_order_lineitem (
    clin_order_id int,
    clin_line_number int,
    clin_item_id char(16),
    clin_promotion_id char(16),
    clin_quantity int,
    clin_sales_price decimal(7,2),
    clin_coupon_amt decimal(7,2),
    clin_warehouse_id char(16),
    clin_ship_date char(10),
    clin_catalog_number int,
    clin_catalog_page_number int,
    clin_ship_cost decimal(7,2))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_catalog_order_lineitem';


DROP TABLE IF EXISTS s_catalog_returns;

CREATE EXTERNAL TABLE s_catalog_returns (
    cret_call_center_id char(16),
    cret_order_id int,
    cret_line_number int,
    cret_item_id char(16),
    cret_return_customer_id char(16),
    cret_refund_customer_id char(16),
    cret_return_date char(10),
    cret_return_time char(10),
    cret_return_qty int,
    cret_return_amt decimal(7,2),
    cret_return_tax decimal(7,2),
    cret_return_fee decimal(7,2),
    cret_return_ship_cost decimal(7,2),
    cret_refunded_cash decimal(7,2),
    cret_reversed_charge decimal(7,2),
    cret_merchant_credit decimal(7,2),
    cret_reason_id char(16),
    cret_shipmode_id char(16),
    cret_catalog_page_id char(16),
    cret_warehouse_id char(16))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_catalog_returns';


DROP TABLE IF EXISTS s_inventory;

CREATE EXTERNAL TABLE s_inventory (
    invn_warehouse_id char(16),
    invn_item_id char(16),
    invn_date char(10),
    invn_qty_on_hand int)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_inventory';


DROP TABLE IF EXISTS s_purchase_lineitem;

CREATE EXTERNAL TABLE s_purchase_lineitem (
    plin_purchase_id int,
    plin_line_number int,
    plin_item_id char(16),
    plin_promotion_id char(16),
    plin_quantity int,
    plin_sale_price float,
    plin_coupon_amt float,
    plin_comment char(100))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_purchase_lineitem';


DROP TABLE IF EXISTS s_purchase;

CREATE EXTERNAL TABLE s_purchase (
    purc_purchase_id int,
    purc_store_id char(16),
    purc_customer_id char(16),
    purc_purchase_date char(10),
    purc_purchase_time int,
    purc_register_id int,
    purc_clerk_id int,
    purc_comment char(100))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_purchase';


DROP TABLE IF EXISTS s_web_order;

CREATE EXTERNAL TABLE s_web_order (
    word_order_id int,
    word_bill_customer_id char(16),
    word_ship_customer_id char(16),
    word_order_date char(10),
    word_order_time int,
    word_ship_mode_id char(16),
    word_web_site_id char(16),
    word_order_comments char(100))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_web_order';


DROP TABLE IF EXISTS s_web_order_lineitem;

CREATE EXTERNAL TABLE s_web_order_lineitem (
    wlin_order_id int,
    wlin_line_number int,
    wlin_item_id char(16),
    wlin_promotion_id char(16),
    wlin_quantity int,
    wlin_sales_price float,
    wlin_coupon_amt float,
    wlin_warehouse_id char(16),
    wlin_ship_date char(10),
    wlin_ship_cost float,
    wlin_web_page_id char(16))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_web_order_lineitem';


DROP TABLE IF EXISTS s_web_returns;

CREATE EXTERNAL TABLE s_web_returns (
    wret_web_page_id char(16),
    wret_order_id int,
    wret_line_number int,
    wret_item_id char(16),
    wret_return_customer_id char(16),
    wret_refund_customer_id char(16),
    wret_return_date char(10),
    wret_return_time char(10),
    wret_return_qty int,
    wret_return_amt decimal(7,2),
    wret_return_tax decimal(7,2),
    wret_return_fee decimal(7,2),
    wret_return_ship_cost decimal(7,2),
    wret_refunded_cash decimal(7,2),
    wret_reversed_charge decimal(7,2),
    wret_account_credit decimal(7,2),
    wret_reason_id char(16))
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES(
    'field.delim'='|',
    'serialization.format'='|')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    '${LOCATION}/s_web_returns';


DROP TABLE IF EXISTS inventory_delete_date;

CREATE EXTERNAL TABLE inventory_delete_date (
  from_dt date,
  to_dt date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${LOCATION}/inventory_delete';


DROP TABLE IF EXISTS delete_date;

CREATE EXTERNAL TABLE delete_date (
  from_dt date,
  to_dt date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${LOCATION}/delete';


---------------------------------------------------------------------------------------

-- this view is created in update_set_x
-- the dimension tables are in a different database

DROP VIEW IF EXISTS srv;

CREATE VIEW srv AS
SELECT
    t_time_sk sr_return_time_sk,
    i_item_sk sr_item_sk,
    c_customer_sk sr_customer_sk,
    c_current_cdemo_sk sr_cdemo_sk,
    c_current_hdemo_sk sr_hdemo_sk,
    c_current_addr_sk sr_addr_sk,
    s_store_sk sr_store_sk,
    r_reason_sk sr_reason_sk,
    sret_ticket_number sr_ticket_number,
    sret_return_qty sr_return_quantity,
    sret_return_amount sr_return_amt,
    sret_return_tax sr_return_tax,
    sret_return_amount + sret_return_tax sr_return_amt_inc_tax,
    sret_return_fee sr_fee,
    sret_return_ship_cost sr_return_ship_cost,
    sret_refunded_cash sr_refunded_cash,
    sret_reversed_charge sr_reversed_charge,
    sret_store_credit sr_store_credit,
    sret_return_amount + sret_return_tax + sret_return_fee - sret_refunded_cash - sret_reversed_charge - sret_store_credit sr_net_loss,
    d_date_sk sr_returned_date_sk
FROM s_store_returns
LEFT OUTER JOIN ${DB_DIMS}.date_dim
    ON (cast(sret_return_date as date) = d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON (( cast(substr(sret_return_time,1,2) AS integer) * 3600 + cast(substr(sret_return_time,4,2) AS integer) * 60 + cast(substr(sret_return_time,7,2) AS integer)) = t_time)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (sret_item_id = i_item_id)
LEFT OUTER JOIN ${DB_DIMS}.customer
    ON (sret_customer_id = c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.store
    ON (sret_store_id = s_store_id)
LEFT OUTER JOIN ${DB_DIMS}.reason
    ON (sret_reason_id = r_reason_id)
WHERE i_rec_end_date = '' AND s_rec_end_date = '';


DROP VIEW IF EXISTS ssv;

CREATE VIEW ssv AS
SELECT
    t_time_sk ss_sold_time_sk,
    i_item_sk ss_item_sk,
    c_customer_sk ss_customer_sk,
    c_current_cdemo_sk ss_cdemo_sk,
    c_current_hdemo_sk ss_hdemo_sk,
    c_current_addr_sk ss_addr_sk,
    s_store_sk ss_store_sk,
    p_promo_sk ss_promo_sk,
    purc_purchase_id ss_ticket_number,
    plin_quantity ss_quantity,
    i_wholesale_cost ss_wholesale_cost,
    i_current_price ss_list_price,
    plin_sale_price ss_sales_price,
    (i_current_price - plin_sale_price) * plin_quantity ss_ext_discount_amt,
    plin_sale_price * plin_quantity ss_ext_sales_price,
    i_wholesale_cost * plin_quantity ss_ext_wholesale_cost,
    i_current_price * plin_quantity ss_ext_list_price,
    i_current_price * s_tax_precentage ss_ext_tax,
    plin_coupon_amt ss_coupon_amt,
    (plin_sale_price * plin_quantity) - plin_coupon_amt ss_net_paid,
    ((plin_sale_price * plin_quantity) - plin_coupon_amt) * (1 + s_tax_precentage) ss_net_paid_inc_tax,
    ((plin_sale_price * plin_quantity) - plin_coupon_amt) - (plin_quantity * i_wholesale_cost) ss_net_profit,
    d_date_sk ss_sold_date_sk
FROM s_purchase
LEFT OUTER JOIN ${DB_DIMS}.customer
    ON (purc_customer_id = c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.store
    ON (purc_store_id = s_store_id)
LEFT OUTER JOIN ${DB_DIMS}.date_dim
    ON (cast(purc_purchase_date as date) = d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON (PURC_PURCHASE_TIME = t_time)
JOIN s_purchase_lineitem
    ON (purc_purchase_id = plin_purchase_id)
LEFT OUTER JOIN ${DB_DIMS}.promotion
    ON plin_promotion_id = p_promo_id
LEFT OUTER JOIN ${DB_DIMS}.item
    ON plin_item_id = i_item_id
WHERE purc_purchase_id = plin_purchase_id AND i_rec_end_date = '' AND s_rec_end_date = '';


DROP VIEW IF EXISTS wsv;

CREATE VIEW wsv AS
SELECT
    t_time_sk ws_sold_time_sk,
    d2.d_date_sk ws_ship_date_sk,
    i_item_sk ws_item_sk,
    c1.c_customer_sk ws_bill_customer_sk,
    c1.c_current_cdemo_sk ws_bill_cdemo_sk,
    c1.c_current_hdemo_sk ws_bill_hdemo_sk,
    c1.c_current_addr_sk ws_bill_addr_sk,
    c2.c_customer_sk ws_ship_customer_sk,
    c2.c_current_cdemo_sk ws_ship_cdemo_sk,
    c2.c_current_hdemo_sk ws_ship_hdemo_sk,
    c2.c_current_addr_sk ws_ship_addr_sk,
    wp_web_page_sk ws_web_page_sk,
    web_site_sk ws_web_site_sk,
    sm_ship_mode_sk ws_ship_mode_sk,
    w_warehouse_sk ws_warehouse_sk,
    p_promo_sk ws_promo_sk,
    word_order_id ws_order_number,
    wlin_quantity ws_quantity,
    i_wholesale_cost ws_wholesale_cost,
    i_current_price ws_list_price,
    wlin_sales_price ws_sales_price,
    (i_current_price-wlin_sales_price) * wlin_quantity ws_ext_discount_amt,
    wlin_sales_price * wlin_quantity ws_ext_sales_price,
    i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost,
    i_current_price * wlin_quantity ws_ext_list_price,
    i_current_price * web_tax_percentage ws_ext_tax,
    wlin_coupon_amt ws_coupon_amt,
    wlin_ship_cost * wlin_quantity ws_ext_ship_cost,
    (wlin_sales_price * wlin_quantity) - wlin_coupon_amt ws_net_paid,
    ((wlin_sales_price * wlin_quantity) - wlin_coupon_amt) * (1 + web_tax_percentage) ws_net_paid_inc_tax,
    ((wlin_sales_price * wlin_quantity) - wlin_coupon_amt) - (wlin_quantity * i_wholesale_cost) ws_net_paid_inc_ship,
    (wlin_sales_price * wlin_quantity) - wlin_coupon_amt + (wlin_ship_cost * wlin_quantity) + i_current_price * web_tax_percentage ws_net_paid_inc_ship_tax,
    ((wlin_sales_price * wlin_quantity) - wlin_coupon_amt) - (i_wholesale_cost * wlin_quantity) ws_net_profit,
    d1.d_date_sk ws_sold_date_sk
FROM s_web_order
LEFT OUTER JOIN ${DB_DIMS}.date_dim d1
    ON (cast(word_order_date as date) = d1.d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON (word_order_time = t_time)
LEFT OUTER JOIN ${DB_DIMS}.customer c1
    ON (word_bill_customer_id = c1.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c2
    ON (word_ship_customer_id = c2.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.web_site
    ON (word_web_site_id = web_site_id AND web_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.ship_mode
    ON (word_ship_mode_id = sm_ship_mode_id)
JOIN s_web_order_lineitem
    ON (word_order_id = wlin_order_id)
LEFT OUTER JOIN ${DB_DIMS}.date_dim d2
    ON (cast(wlin_ship_date as date) = d2.d_date)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (wlin_item_id = i_item_id AND i_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.web_page
    ON (wlin_web_page_id = wp_web_page_id AND wp_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.warehouse
    ON (wlin_warehouse_id = w_warehouse_id)
LEFT OUTER JOIN ${DB_DIMS}.promotion
    ON (wlin_promotion_id = p_promo_id);


DROP VIEW IF EXISTS wrv;

CREATE VIEW wrv AS
SELECT
    t_time_sk wr_return_time_sk,
    i_item_sk wr_item_sk,
    c1.c_customer_sk wr_refunded_customer_sk,
    c1.c_current_cdemo_sk wr_refunded_cdemo_sk,
    c1.c_current_hdemo_sk wr_refunded_hdemo_sk,
    c1.c_current_addr_sk wr_refunded_addr_sk,
    c2.c_customer_sk wr_returning_customer_sk,
    c2.c_current_cdemo_sk wr_returning_cdemo_sk,
    c2.c_current_hdemo_sk wr_returning_hdemo_sk,
    c2.c_current_addr_sk wr_returing_addr_sk,
    wp_web_page_sk wr_web_page_sk,
    r_reason_sk wr_reason_sk,
    wret_order_id wr_order_number,
    wret_return_qty wr_return_quantity,
    wret_return_amt wr_return_amt,
    wret_return_tax wr_return_tax,
    wret_return_amt + wret_return_tax AS wr_return_amt_inc_tax,
    wret_return_fee wr_fee,
    wret_return_ship_cost wr_return_ship_cost,
    wret_refunded_cash wr_refunded_cash,
    wret_reversed_charge wr_reversed_charge,
    wret_account_credit wr_account_credit,
    wret_return_amt + wret_return_tax + wret_return_fee - wret_refunded_cash -     wret_reversed_charge - wret_account_credit wr_net_loss,
    d_date_sk wr_return_date_sk
FROM s_web_returns
LEFT OUTER JOIN ${DB_DIMS}.date_dim
    ON (cast(wret_return_date as date) = d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON ((CAST(SUBSTR(wret_return_time,1,2) AS integer) * 3600  + CAST(SUBSTR(wret_return_time,4,2) AS integer) * 60+CAST(SUBSTR(wret_return_time,7,2) AS integer)) = t_time)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (wret_item_id = i_item_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c1
    ON (wret_return_customer_id = c1.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c2
    ON (wret_refund_customer_id = c2.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.reason
    ON (wret_reason_id = r_reason_id)
LEFT OUTER JOIN ${DB_DIMS}.web_page
    ON (wret_web_page_id = WP_WEB_PAGE_id)
WHERE i_rec_end_date = '' AND wp_rec_end_date = '';


DROP VIEW IF EXISTS csv;

CREATE VIEW csv AS
SELECT
    t_time_sk cs_sold_time_sk,
    d2.d_date_sk cs_ship_date_sk,
    c1.c_customer_sk cs_bill_customer_sk,
    c1.c_current_cdemo_sk cs_bill_cdemo_sk,
    c1.c_current_hdemo_sk cs_bill_hdemo_sk,
    c1.c_current_addr_sk cs_bill_addr_sk,
    c2.c_customer_sk cs_ship_customer_sk,
    c2.c_current_cdemo_sk cs_ship_cdemo_sk,
    c2.c_current_hdemo_sk cs_ship_hdemo_sk,
    c2.c_current_addr_sk cs_ship_addr_sk,
    cc_call_center_sk cs_call_center_sk,
    cp_catalog_page_sk cs_catalog_page_sk,
    sm_ship_mode_sk cs_ship_mode_sk,
    w_warehouse_sk cs_warehouse_sk,
    i_item_sk cs_item_sk,
    p_promo_sk cs_promo_sk,
    cord_order_id cs_order_number,
    clin_quantity cs_quantity,
    i_wholesale_cost cs_wholesale_cost,
    i_current_price cs_list_price,
    clin_sales_price cs_sales_price,
    (i_current_price - clin_sales_price) * clin_quantity cs_ext_discount_amt,
    clin_sales_price * clin_quantity cs_ext_sales_price,
    i_wholesale_cost * clin_quantity cs_ext_wholesale_cost,
    i_current_price * clin_quantity cs_ext_list_price,
    i_current_price * cc_tax_percentage cs_ext_tax,
    clin_coupon_amt cs_coupon_amt,
    clin_ship_cost * clin_quantity cs_ext_ship_cost,
    (clin_sales_price * clin_quantity) - clin_coupon_amt cs_net_paid,
    ((clin_sales_price * clin_quantity) - clin_coupon_amt) * (1 + cc_tax_percentage) cs_net_paid_inc_tax,
    (clin_sales_price * clin_quantity) - clin_coupon_amt + (clin_ship_cost * clin_quantity) cs_net_paid_inc_ship,
    (clin_sales_price * clin_quantity) - clin_coupon_amt + (clin_ship_cost * clin_quantity) + i_current_price * cc_tax_percentage cs_net_paid_inc_ship_tax,
    ((clin_sales_price * clin_quantity) - clin_coupon_amt) - (clin_quantity * i_wholesale_cost) cs_net_profit,
    d1.d_date_sk cs_sold_date_sk
FROM s_catalog_order
LEFT OUTER JOIN ${DB_DIMS}.date_dim d1
    ON (cast(cord_order_date as date) = d1.d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON (cord_order_time = t_time)
LEFT OUTER JOIN ${DB_DIMS}.customer c1
    ON (cord_bill_customer_id = c1.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c2
    ON (cord_ship_customer_id = c2.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.call_center
    ON (cord_call_center_id = cc_call_center_id AND cc_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.ship_mode
    ON (cord_ship_mode_id = sm_ship_mode_id)
JOIN s_catalog_order_lineitem
    ON (cord_order_id = clin_order_id)
LEFT OUTER JOIN ${DB_DIMS}.date_dim d2
    ON (cast(clin_ship_date as date) = d2.d_date)
LEFT OUTER JOIN ${DB_DIMS}.catalog_page
    ON (clin_catalog_page_number = cp_catalog_page_number AND clin_catalog_number = cp_catalog_number)
LEFT OUTER JOIN ${DB_DIMS}.warehouse
    ON (clin_warehouse_id = w_warehouse_id)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (clin_item_id = i_item_id AND i_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.promotion
    ON (clin_promotion_id = p_promo_id);


DROP VIEW IF EXISTS crv;

CREATE VIEW crv AS
SELECT
    t_time_sk cr_return_time_sk,
    i_item_sk cr_item_sk,
    c1.c_customer_sk cr_refunded_customer_sk,
    c1.c_current_cdemo_sk cr_refunded_cdemo_sk,
    c1.c_current_hdemo_sk cr_refunded_hdemo_sk,
    c1.c_current_addr_sk cr_refunded_addr_sk,
    c2.c_customer_sk cr_returning_customer_sk,
    c2.c_current_cdemo_sk cr_returning_cdemo_sk,
    c2.c_current_hdemo_sk cr_returning_hdemo_sk,
    c2.c_current_addr_sk cr_returing_addr_sk,
    cc_call_center_sk cr_call_center_sk,
    cp_catalog_page_sk cr_catalog_page_sk,
    sm_ship_mode_sk cr_ship_mode_sk,
    w_warehouse_sk cr_warehouse_sk,
    r_reason_sk cr_reason_sk,
    cret_order_id cr_order_number,
    cret_return_qty cr_return_quantity,
    cret_return_amt cr_return_amt,
    cret_return_tax cr_return_tax,
    cret_return_amt + cret_return_tax as cr_return_amt_inc_tax,
    cret_return_fee cr_fee,
    cret_return_ship_cost cr_return_ship_cost,
    cret_refunded_cash cr_refunded_cash,
    cret_reversed_charge cr_reversed_charge,
    cret_merchant_credit cr_merchant_credit,
    cret_return_amt + cret_return_tax + cret_return_fee - cret_refunded_cash - cret_reversed_charge - cret_merchant_credit cr_net_loss,
    d_date_sk cr_return_date_sk
FROM s_catalog_returns
LEFT OUTER JOIN ${DB_DIMS}.date_dim
    ON (cast(cret_return_date as date) = d_date)
LEFT OUTER JOIN ${DB_DIMS}.time_dim
    ON ((CAST(substr(cret_return_time,1,2) AS integer) * 3600 + CAST(substr(cret_return_time,4,2) AS integer) * 60 + CAST(substr(cret_return_time,7,2) AS integer)) = t_time)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (cret_item_id = i_item_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c1
    ON (cret_return_customer_id = c1.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.customer c2
    ON (cret_refund_customer_id = c2.c_customer_id)
LEFT OUTER JOIN ${DB_DIMS}.reason
    ON (cret_reason_id = r_reason_id)
LEFT OUTER JOIN ${DB_DIMS}.call_center
    ON (cret_call_center_id = cc_call_center_id)
LEFT OUTER JOIN ${DB_DIMS}.catalog_page
    ON (cret_catalog_page_id = cp_catalog_page_id)
LEFT OUTER JOIN ${DB_DIMS}.ship_mode
    ON (cret_shipmode_id = sm_ship_mode_id)
LEFT OUTER JOIN ${DB_DIMS}.warehouse
    ON (cret_warehouse_id = w_warehouse_id)
WHERE i_rec_end_date = '' AND cc_rec_end_date = '';


DROP VIEW IF EXISTS iv;

CREATE VIEW iv AS
SELECT
    d_date_sk inv_date_sk,
    i_item_sk inv_item_sk,
    w_warehouse_sk inv_warehouse_sk,
    invn_qty_on_hand inv_quantity_on_hand
FROM s_inventory
LEFT OUTER JOIN ${DB_DIMS}.warehouse
    ON (invn_warehouse_id = w_warehouse_id)
LEFT OUTER JOIN ${DB_DIMS}.item
    ON (invn_item_id = i_item_id AND i_rec_end_date = '')
LEFT OUTER JOIN ${DB_DIMS}.date_dim
    ON (d_date = invn_date);
