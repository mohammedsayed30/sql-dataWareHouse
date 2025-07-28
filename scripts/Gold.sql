--Building Customer demination from crm and erp system


CREATE OR REPLACE VIEW gold.dim_customers AS   --to use whenever i want

SELECT 
ROW_NUMBER() OVER (ORDER BY CST_ID) AS customer_key,
CI.CST_ID AS customer_id,
CI.CST_KEY AS customer_number,
CI.CST_FIRSTNAME AS first_name,
CI.CST_LASTNAME AS last_name,
EL.CNTRY AS country,
CASE 
WHEN CI.CST_GNDR != 'N/A' THEN CI.CST_GNDR
ELSE NVL(EC.GEN, 'N/A') 
END AS gender,
EC.BDATE AS birth_date,
CI.CST_MARITAL_STATUS AS marital_status,
CI.CST_CREATE_DATE AS created_date

FROM SILVER.CRM_CUST_INFO  CI 

LEFT JOIN SILVER.ERP_CUST_AZ12 EC ON CI.CST_KEY = EC.CUST_KEY
LEFT JOIN SILVER.ERP_LOC_A101 EL ON CI.CST_KEY = EL.CUST_KEY;


--Building Product dimitation from crm  and erp



--create view for demination product in the gold layer
CREATE OR REPLACE VIEW gold.dim_products AS

SELECT
ROW_NUMBER() OVER (ORDER BY PRD_ID) AS product_primary_key,
CP.PRD_ID AS product_id,
CP.PRD_KEY AS product_key,
CP.PRD_NM AS product_number,
CP.PRD_COST AS product_price,
CP.PRD_LINE AS product_line,
CP.CAT_ID AS category_id,
EP.CAT AS category_name,
EP.SUBCAT AS sub_category_name,
EP.MAINTENANCE AS maintenance,
CP.PRD_START_DT AS product_start_date

FROM SILVER.CRM_PRD_INFO CP  LEFT JOIN SILVER.ERP_PX_CAT_G1V2 EP ON  CP.CAT_ID = EP.ID 
WHERE CP.PRD_END_DT IS NULL; --only the current product not the end ones 



--create view for Fact sales in the gold layer 

CREATE OR REPLACE VIEW gold.fact_sales AS

SELECT 
SL.SLS_ORD_NUM AS order_number,
SP.product_primary_key as product_key,
SC.customer_key as customer_key,
SL.SLS_SALES as sales_price,
SL.SLS_QUANTITY as sales_quantity,
SL.SLS_PRICE as product_price,
SL.SLS_ORDER_DT as order_date,
SL.SLS_SHIP_DT as order_shipping_date,
SL.SLS_DUE_DT as order_due_date

FROM SILVER.CRM_SALES_DETAILS SL
LEFT JOIN GOLD.dim_products SP ON SL.SLS_PRD_KEY=SP.PRODUCT_KEY
LEFT JOIN GOLD.dim_customers SC ON SL.SLS_CUST_ID=SC.CUSTOMER_ID;


-------
