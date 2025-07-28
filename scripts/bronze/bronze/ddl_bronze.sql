--all sql scripts used to create bronze schema and the tables in oracle

--bronze Schema
CREATE USER bronze IDENTIFIED BY bronze123;
GRANT CONNECT, RESOURCE TO bronze;
ALTER USER bronze TEMPORARY TABLESPACE temp;
ALTER USER bronze QUOTA UNLIMITED ON users;


--create tables for bronze schema


--first create table for each crm csv files

--cust_info

CREATE TABLE bronze.crm_cust_info (
  cst_id INT,
  cst_key NVARCHAR2(50),
  cst_firstname NVARCHAR2(50),
  cst_lastname NVARCHAR2(50),
  cst_marital_status NVARCHAR2(50),
  cst_gndr NVARCHAR2(50),
  cst_create_date DATE
);


--prd_info

CREATE TABLE bronze.crm_prd_info (
  prd_id INT,
  prd_key NVARCHAR2(50),
  prd_nm NVARCHAR2(50),
  prd_cost INT,
  prd_line NVARCHAR2(50),
  prd_start_dt DATE,
  prd_end_dt DATE
);

--sales details

CREATE TABLE bronze.crm_sales_details (
  sls_ord_num NVARCHAR2(50),
  sls_prd_key NVARCHAR2(50),
  sls_cust_id INT,
  sls_order_dt INT,
  sls_ship_dt INT,
  sls_due_dt INT,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT
);

--ERP source information

--CUST_AZ12

CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR2(50),
    bdate DATE,
    gen NVARCHAR2(50)
);

--CUST_AZ12

CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR2(50),
    cntry NVARCHAR2(50)
);

--PX_CAT_G1V2

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR2(50),
    cat NVARCHAR2(50),
    subcat NVARCHAR2(50),
    maintenance NVARCHAR2(50)
);

--after that you can load the data from the source manually



