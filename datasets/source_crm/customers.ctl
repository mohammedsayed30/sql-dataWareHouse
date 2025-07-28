LOAD DATA
INFILE 'F:\WareHouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
INTO TABLE bronze.crm_cust_info
APPEND
SKIP 1
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date DATE "YYYY-MM-DD"
)
