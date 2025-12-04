

create or alter procedure silver.load_silver as
begin
print'>>silver.crm_cust_info'
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
             WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
             ELSE 'n/a'
        END AS cst_marital_status,
        CASE WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
             WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
             ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id, cst_key 
            ORDER BY cst_create_date DESC
        ) AS flag_list
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL AND cst_key IS NOT NULL
) t
WHERE flag_list = 1;







print'>>silver.crm_prd_info'
insert into silver.crm_prd_info(
  prd_id ,
  id_key,
  prd_key ,
  prd_nm,
  prd_cost ,
  prd_line,
  prd_start_dt,
  prd_end_dt)
select
prd_id,
replace(substring(prd_key,1,5),'-','_')as id_key,
       substring(prd_key,7,len(prd_key))as prd_key,
prd_nm,
isnull(prd_cost,0)as prd_cost,
case upper(trim(prd_line))
when 'R' then 'Road'
when 'S' then 'Other Sales'
 when 'M' then 'Mountain'
when 'T' then 'Touring'
	 else 'n/a'
		  end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
 DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt 
 from( select*,
       ROW_NUMBER() OVER(PARTITION BY prd_key ORDER BY prd_key) as one
FROM bronze.crm_prd_info)t
where one=1




print '>>silver.crm_sales_details'
insert into silver.crm_sales_details(
 sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price )
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
      when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
      else cast(cast(sls_order_dt as varchar) as date)
      end as sls_order_dt,
cast(cast(sls_ship_dt as varchar) as date) as sls_ship_dt,
cast(cast(sls_due_dt as varchar) as date) as sls_due_dt,
  case 
        when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
            then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,
	sls_quantity,
    case 
        when sls_price <= 0 or sls_price is null
            then sls_sales / nullif(sls_quantity, 0)
        else sls_price
    end as sls_price
from bronze.crm_sales_details


print '>>silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12(
    CID,
    BDATE,
    gen)
SELECT 
    CID,
    BDATE,
    gen
FROM (
    SELECT 
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
             ELSE CID 
        END AS CID,
        CASE WHEN BDATE > GETDATE() THEN NULL
             ELSE BDATE
        END AS BDATE,
        CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
             ELSE 'n/a'
        END AS gen,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
                 ELSE CID 
            END 
            ORDER BY (SELECT NULL)) as rn
    FROM bronze.erp_cust_az12
) t 
WHERE rn = 1;

print '>>silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT 
    REPLACE(CID, '-', '') AS CID,
    CASE 
        WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
        WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
        WHEN CNTRY IS NULL OR CNTRY = '' THEN 'n/a'
        ELSE CNTRY
    END AS CNTRY
FROM (
    SELECT 
        CASE 
            WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
            ELSE CID 
        END AS CID,
        CNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
                    ELSE CID 
                END 
            ORDER BY CID
        ) AS rn
    FROM bronze.erp_loc_a101
) t
WHERE rn = 1;


print '>>silver.erp_px_cat_g1v2'
insert into silver.erp_px_cat_g1v2(
  ID ,
  CAT ,
  SUBCAT,
  MAINTENANCE 
)
SELECT 
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY ID ORDER BY ID) as one
    FROM bronze.erp_px_cat_g1v2
) t
WHERE one = 1

end
