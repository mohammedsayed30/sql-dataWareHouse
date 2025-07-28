-- Create silver schema
CREATE USER silver IDENTIFIED BY 123456;
GRANT CONNECT, RESOURCE TO silver;
ALTER USER silver DEFAULT TABLESPACE users;
ALTER USER silver TEMPORARY TABLESPACE temp;
ALTER USER silver QUOTA UNLIMITED ON users;

--create the tables of the silver layer

CREATE TABLE silver.crm_cust_info (
  cst_id INT,
  cst_key NVARCHAR2(50),
  cst_firstname NVARCHAR2(50),
  cst_lastname NVARCHAR2(50),
  cst_marital_status NVARCHAR2(50),
  cst_gndr NVARCHAR2(50),
  cst_create_date DATE,
  dwh_create_date DATE DEFAULT SYSDATE
);

--prd_info
DROP TABLE  silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
  prd_id INT,
  cat_id NVARCHAR2(50),
  prd_key NVARCHAR2(50),
  prd_nm  NVARCHAR2(50),
  prd_cost INT,
  prd_line NVARCHAR2(50),
  prd_start_dt DATE,
  prd_end_dt DATE,
  dwh_create_date DATE DEFAULT SYSDATE
);

--sales details

CREATE TABLE silver.crm_sales_details (
  sls_ord_num NVARCHAR2(50),
  sls_prd_key NVARCHAR2(50),
  sls_cust_id INT,
  sls_order_dt DATE,
  sls_ship_dt DATE,
  sls_due_dt DATE,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT,
  dwh_create_date DATE DEFAULT SYSDATE
);

DROP TABLE silver.crm_sales_details;

--ERP source information

--CUST_AZ12

CREATE TABLE silver.erp_cust_az12 (
    CUST_KEY NVARCHAR2(50),
    bdate DATE,
    gen NVARCHAR2(50),
    dwh_create_date DATE DEFAULT SYSDATE
);

DROP TABLE silver.erp_loc_a101;

--CUST_AZ12

CREATE TABLE silver.erp_loc_a101 (
    CUST_KEY NVARCHAR2(50),
    cntry NVARCHAR2(50),
    dwh_create_date DATE DEFAULT SYSDATE
);

--PX_CAT_G1V2

CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR2(50),
    cat NVARCHAR2(50),
    subcat NVARCHAR2(50),
    maintenance NVARCHAR2(50),
    dwh_create_date DATE DEFAULT SYSDATE
);
