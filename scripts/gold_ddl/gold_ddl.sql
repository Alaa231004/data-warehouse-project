
create view gold.dim_products as
select
row_number()over(order by prd_start_dt, prd_key)as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.id_key as category_id,
px.CAT as category,
px.SUBCAT as subcategory,
px.MAINTENANCE,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 px
on px.ID=pn.id_key
where prd_end_dt is null

create view gold.dim_customers as
select 
row_number()over(order by cst_id)as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
lo.CNTRY as country,

case when ci.cst_gndr!='n/a' then ci.cst_gndr
     else coalesce(cu.GEN,'n/a')
	 end as Gender,
	 ci.cst_marital_status as marital_status,
	 cu.BDATE as birthdate,
ci.cst_create_date as create_date

from silver.crm_cust_info ci
left join silver.erp_cust_az12 cu
on ci.cst_key=cu.CID
left join silver.erp_loc_a101 lo
on ci.cst_key=lo.CID



create view gold.fact_sales as

select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on pr.product_number=sd.sls_prd_key
left join gold.dim_customers cu
on cu.customer_id=sd.sls_cust_id
