-------------------------------------------------------------------
--transform all bronze table   to silver layer after transformation
-------------------------------------------------------------------


------------------------------------------------------
--transform crm_cust_info from bronze to silver layer
------------------------------------------------------
CREATE OR REPLACE PROCEDURE silver.load_silver AS
BEGIN

EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.crm_cust_info';

INSERT INTO SILVER.crm_cust_info(
CST_ID,
CST_KEY,
CST_FIRSTNAME,
CST_LASTNAME,
CST_MARITAL_STATUS,
CST_GNDR,
CST_CREATE_DATE
)
SELECT
  t.CST_ID,
  t.CST_KEY,
  --remove spaces
  TRIM(t.CST_FIRSTNAME) AS CST_FIRSTNAME,  
  TRIM(t.CST_LASTNAME) AS CST_LASTNAME,
  --user freindly of the marital status
  CASE
    WHEN UPPER(TRIM(t.CST_MARITAL_STATUS)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(t.CST_MARITAL_STATUS)) = 'M' THEN 'Married'
    ELSE 'N/A'
  END AS CST_MARITAL_STATUS,
  --user freindly of the gender
  CASE
    WHEN UPPER(TRIM(t.CST_GNDR)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(t.CST_GNDR)) = 'M' THEN 'Male'
    ELSE 'N/A'
  END AS CST_GNDR,

  t.CST_CREATE_DATE
--remove the null and repeated the customer ids
FROM (
  SELECT t.*,
         ROW_NUMBER() OVER (
           PARTITION BY CST_ID
           ORDER BY CST_CREATE_DATE DESC
         ) AS rn
  FROM BRONZE.crm_cust_info t
  WHERE CST_ID IS NOT NULL
) t
WHERE t.rn = 1;



------------------------------------------------------
--transform crm_prd_info from bronze to silver layer
------------------------------------------------------

EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.crm_prd_info';

insert into silver.crm_prd_info(
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
)

SELECT 
  --remove spaces
  CRM.PRD_ID AS PRD_ID,
  --extract the product key and the category for relationship
  SUBSTR(CRM.PRD_KEY,1,5) AS cat_id,
  SUBSTR(CRM.PRD_KEY,7) AS prd_key,
  
  --remove spaces
  TRIM(CRM.PRD_NM) AS PRD_NM,
  NVL(CRM.PRD_COST,0) AS PRD_COST,
  --user freindly of the Road line status
  CASE UPPER(CAST(CRM.PRD_LINE AS VARCHAR2(10)))
    WHEN 'R' THEN 'Road'
    WHEN 'M' THEN 'Mountain'
    WHEN 'T' THEN 'Touring'
    WHEN 'S' THEN 'Other Sales'
    ELSE 'N/A'
  END AS PRD_LINE,
  TRIM(CRM.PRD_START_DT),
  LEAD(CRM.PRD_START_DT) OVER (PARTITION BY PRD_KEY ORDER BY CRM.PRD_START_DT)-1 AS PRD_END_DT
FROM bronze.crm_prd_info CRM;


--transform the sales from bronze layer to silver layer 

EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.crm_sales_details';

INSERT INTO silver.crm_sales_details(
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
SELECT 
--THE FIRST THREE COLUMNS NO ERRORS 
SLS_ORD_NUM,
SLS_PRD_KEY,
SLS_CUST_ID,

--CONVERT TO DATE
CASE
  WHEN SLS_ORDER_DT=0 OR LENGTH(SLS_ORDER_DT) !=8   THEN NULL
ELSE
  TO_DATE(TO_CHAR(SLS_ORDER_DT), 'YYYYMMDD')
END AS SLS_ORDER_DT,
--CONVERT TO DATE
CASE
  WHEN SLS_SHIP_DT=0 OR LENGTH(SLS_SHIP_DT) !=8 THEN NULL
ELSE
  TO_DATE(TO_CHAR(SLS_SHIP_DT), 'YYYYMMDD')
END AS SLS_SHIP_DT,
--CONVERT TO DATE
CASE
  WHEN SLS_DUE_DT=0 OR LENGTH(SLS_DUE_DT)!=8 THEN NULL
ELSE
  TO_DATE(TO_CHAR(SLS_DUE_DT), 'YYYYMMDD')
END AS SLS_DUE_DT,

--CHECK FOR INTEGRITY OF the sales
CASE 
  WHEN SLS_SALES IS NULL OR SLS_SALES<=0 OR SLS_SALES != ABS(SLS_PRICE) * SLS_QUANTITY THEN ABS(SLS_PRICE) * SLS_QUANTITY
ELSE
  SLS_SALES 
END AS SLS_SALES,

--quantity is good
SLS_QUANTITY,

--check for price integrity
CASE
 WHEN SLS_PRICE <=0 OR SLS_PRICE IS NULL  THEN  SLS_SALES/NULLIF(SLS_QUANTITY,1)
 ELSE
  SLS_PRICE
END AS SLS_PRICE  

FROM BRONZE.CRM_SALES_DETAILS ;




--transform the erp_cust_az12


EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.erp_cust_az12';


INSERT INTO silver.erp_cust_az12(
    CUST_KEY,
    BDATE,
    GEN
)
SELECT 
CASE
WHEN CID LIKE 'NAS%' THEN SUBSTR(CID,4)
ELSE CID
END AS CUT_KEY
,
CASE 
WHEN BDATE > TRUNC(SYSDATE) THEN NULL
ELSE BDATE 
END AS BDATE,

CASE 
WHEN TRIM(GEN)='F' THEN  N'Female'
WHEN TRIM(GEN)='M' THEN  N'Male'
WHEN GEN IS NULL   THEN  N'N/A'
ELSE TRIM(GEN)
END AS GEN 

FROM BRONZE.erp_cust_az12;

--transform ERP_LOC_A101

EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.ERP_LOC_A101';

INSERT INTO SILVER.ERP_LOC_A101(
    CUST_KEY,
    CNTRY
)
SELECT REPLACE(CID,'-','') AS CUST_KEY,
CASE
    WHEN TRIM(CNTRY) = 'DE' THEN N'GERMAN'
    WHEN TRIM(CNTRY) IN ('US','USA') THEN N'United State'
    WHEN CNTRY IS NULL  OR TRIM(CNTRY)=''  THEN N'N/A'
    ELSE TRIM(CNTRY)
END AS CNTRY

FROM BRONZE.ERP_LOC_A101;


--transform ERP_PX_CAT_G1V2

EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.ERP_PX_CAT_G1V2';

INSERT INTO SILVER.ERP_PX_CAT_G1V2 (
    id,
    cat,
    subcat,
    maintenance
)

SELECT ID,CAT,SUBCAT,MAINTENANCE FROM BRONZE.ERP_PX_CAT_G1V2;

END;
