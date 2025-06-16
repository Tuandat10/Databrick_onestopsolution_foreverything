-- create external table metacatalog.goldschema.dim_customer
-- using delta
-- location 
'abfss://gold@osspstorageaccount.dfs.core.windows.net/customer'


-- create external table metacatalog.goldschema.dim_date
-- using delta
-- location 'abfss://gold@osspstorageaccount.dfs.core.windows.net/date'



-- create external table metacatalog.goldschema.dim_paymentmethod
-- using delta
-- location 
'abfss://gold@osspstorageaccount.dfs.core.windows.net/paymentmethod'


-- create external table metacatalog.goldschema.dim_product
-- using delta
-- location 'abfss://gold@osspstorageaccount.dfs.core.windows.net/product'


-- create external table metacatalog.goldschema.dim_promotion
-- using delta
-- location 
'abfss://gold@osspstorageaccount.dfs.core.windows.net/promotion'


-- create external table metacatalog.goldschema.dim_region
-- using delta
-- location 'abfss://gold@osspstorageaccount.dfs.core.windows.net/region'


-- create external table metacatalog.goldschema.dim_shippingmethod
-- using delta
-- location 
'abfss://gold@osspstorageaccount.dfs.core.windows.net/shippingmethod'

-- create external table metacatalog.goldschema.factsales
-- using delta
-- location 
'abfss://gold@osspstorageaccount.dfs.core.windows.net/factsales'

select * from metacatalog.goldschema.dim_customer


