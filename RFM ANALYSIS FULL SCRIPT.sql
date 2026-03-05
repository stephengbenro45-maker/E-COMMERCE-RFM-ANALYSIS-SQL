LOAD DATA LOCAL INFILE 'C:/Users/steph/Downloads/archive (1)/online_retail_cleaned.csv' 
INTO TABLE `rmf analysis data set`.rfm_data_staging 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM `rmf analysis data set`.rfm_data_staging;

LOAD DATA LOCAL INFILE 'C:/Users/steph/Downloads/archive (1)/online_retail_cleaned.csv' 
INTO TABLE `rmf analysis data set`.rfm_data_staging 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE `rmf analysis data set`.rfm_data_staging; 

LOAD DATA LOCAL INFILE 'C:/Users/steph/Downloads/archive (1)/online_retail_cleaned.csv' 
INTO TABLE `rmf analysis data set`.rfm_data_staging 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT COUNT(*) FROM `rmf analysis data set`.rfm_data_staging;

CREATE TABLE rfm_clean AS
SELECT 
    Invoice AS InvoiceNo, 
    StockCode,
    Description,
    CAST(Quantity AS SIGNED) AS Quantity,
    CAST(InvoiceDate AS DATE) AS InvoiceDate,
    CAST(Price AS DECIMAL(10,2)) AS Price,
    Customer_ID AS CustomerID,
    Country,
    (CAST(Quantity AS SIGNED) * CAST(Price AS DECIMAL(10,2))) AS TotalSales
FROM `rmf analysis data set`.rfm_data_staging
WHERE Customer_ID IS NOT NULL AND Customer_ID <> '';


select * from rfm_clean limit 10;

select quantity from `rmf analysis data set`.rfm_clean limit 10;


select customerID,
  datediff((SELECT MAX(invoiceDate) from `rmf analysis data set`.rfm_clean),
  max(invoiceDate)) as recency,
  count(distinct invoiceNo) as frequency,
   sum(quantity * price) as total_sales
   from `rmf analysis data set`.rfm_clean
   group by customerID;
   
   
   WITH VOUCHER AS ( 
   SELECT CUSTOMERID,
   datediff((SELECT MAX(invoiceDate) from `rmf analysis data set`.rfm_clean),
  max(invoiceDate)) as recency,
      Count( distinct invoiceNo) as frequency,
   sum(quantity * price) as total_sales
   from `rmf analysis data set`.rfm_clean
   group by customerID )
 SELECT * FROM VOUCHER;
 
 
 SELECT 
   max(invoiceDate) as end_date, 
   min(invoiceDate) as start_date 
   from `rmf analysis data set`.rfm_clean;
   
   with identify as (
   select count(distinct customerID) AS NUMBER_OF_CUSTOMERS 
   FROM `rmf analysis data set`.rfm_clean)
   SELECT * from identify;
   
   
   
   select `Description`, 
         sum(quantity) as number_of_unit,
         sum(totalsales) as product_profit 
         from `rmf analysis data set`.rfm_clean
         group by `description`
         order by product_profit desc; 
         
         select  CustomerID,
         max(invoiceDate) as last_visit,
         datediff((select  max(invoiceDate) from 
            `rmf analysis data set`.rfm_clean),
             max(invoiceDate)) as days_gone,
          SUM(totalsales) as money_spent
            from 
            `rmf analysis data set`.rfm_clean
		     group by customerID 
             having money_spent > 1000 and days_gone >365
             order by money_spent desc;
             
             
             SELECT 
             distinct customerID, 
              country,
             avg(totalsales) as totals
             from `rmf analysis data set`.rfm_clean
             group by customerID,country
             order by totals desc; 
             
             SELECT 
                 country,
                 sum(totalsales) as money_made 
                 from `rmf analysis data set`.rfm_clean
                  group by country
                  order by money_made desc;
                  
                  select  
                   distinct customerID as customers,
                   count(distinct invoiceNo) as frequency,
                   SUM(quantity) as total_item_bought,
                   sum(totalsales) as monetary
                    from `rmf analysis data set`.rfm_clean
                    group by customers
                    order by frequency desc limit 10;
                    
                    
                    select 
                     customerID as customers,
                    MAX(invoiceDate) as last_visit,
                    count(distinct invoiceNo) as frequency,
                    sum(totalsales) as monetary 
                    FROM  `rmf analysis data set`.rfm_clean
                    group by customers 
                    order by frequency asc;
                    
                    with nurtured as (
                    select CustomerID,
                    MAX(invoiceDate) as last_visit,
                    count(distinct invoiceNo) as frequency,
                    sum(totalsales) as monetary 
                    FROM  `rmf analysis data set`.rfm_clean
                    group by customerID )
                  SELECT * FROM  nurtured
                  ORDER BY frequency asc;
                  
                  
				
                  SELECT 
                  MONTH(invoiceDate) as month_num,
                  monthname(invoiceDate) as month_name,
                  sum(totalsales) as total_sales 
                  from `rmf analysis data set`.rfm_clean
                  where MONTH(invoiceDate) in  (4,12)
                  group by month_num,month_name
                  order by total_sales desc; 
                  
                  
				
                    WITH rfm_base AS (
    SELECT 
        customerID,
        DATEDIFF((SELECT MAX(invoiceDate) FROM rfm_clean), MAX(invoiceDate)) AS recency,
        COUNT(DISTINCT invoiceNo) AS frequency,
        SUM(totalsales) AS monetary
    FROM rfm_clean
    GROUP BY customerID
)
SELECT *,
    NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
FROM rfm_base;

           
           with champion_risk as (
		select distinct customerID,
        COUNT(DISTINCT invoiceNo) AS frequency,
         DATEDIFF((SELECT MAX(invoiceDate) FROM rfm_clean), MAX(invoiceDate)) AS recency,
         sum(totalsales) as money_spent
         from  `rmf analysis data set`.rfm_clean
         group by customerID )
         select * from champion_risk
         order by money_spent desc;
         
         
           
           select customerID,
             sum(totalsales) as revenue 
             from `rmf analysis data set`.rfm_clean
             group by customerID 
             ORDER BY revenue desc limit 10;
             
             select  customerID,
             COUNT(DISTINCT invoiceNo) AS frequency,
             sum(quantity) as purchases
             from `rmf analysis data set`.rfm_clean
             group by customerID
             order by purchases desc;
             
             
           WITH customer_spend AS (
      SELECT 
    customerID,
      SUM(CASE 
        WHEN invoiceDate >= DATE_SUB((SELECT MAX(invoiceDate) FROM rfm_clean), INTERVAL 3 MONTH)
        THEN totalsales 
        ELSE 0
    END) AS last_3_months,

    SUM(CASE 
        WHEN invoiceDate < DATE_SUB((SELECT MAX(invoiceDate) FROM rfm_clean), INTERVAL 3 MONTH)
        THEN totalsales 
        ELSE 0
    END) AS previous_spend

FROM rfm_clean
GROUP BY customerID
)
SELECT *
FROM customer_spend
WHERE last_3_months < previous_spend * 0.6;


select  
       customerID,
         COUNT(DISTINCT invoiceNo) AS frequency,
         SUM(totalsales) as total_revenue
         from `rmf analysis data set`.rfm_clean
         group by customerID
         having  COUNT(DISTINCT invoiceNo) > 5
         order by total_revenue desc;
       
               
             
        
         select
          case 
              when  COUNT(distinct invoiceNo) = 1 then `one_time_buyer `
              when  COUNT(distinct invoiceNo) between 2 and 5 then `ocassional_buyer `
                else 'loyal buyer'
                end as customer_segment, 
                count(customerID) AS TOTAL_CUSTOMERS ,
                SUM(totalsales) as total_revenue 
                group by customer_segment;
                
                
                with customer_frequency as (
    select 
        customerID,
        COUNT(distinct invoiceNo) as frequency,
        SUM(totalsales) as total_sales
    from `rmf analysis data set`.rfm_clean
    group by customerID
)
select 
    case 
        when frequency = 1 then 'one_time_buyer'
        when frequency between 2 and 5 then 'ocassional_buyer'
        else 'loyal_buyer'
    end as customer_segment,
    count(customerID) as total_customers,
    sum(total_sales) as total_sales_per_segment
from customer_frequency
group by customer_segment;



	
             with loyal as ( 
             select customerID,
              COUNT(distinct invoiceNo) as frequency,
               DATEDIFF((SELECT MAX(invoiceDate) FROM rfm_clean), MAX(invoiceDate)) AS recency,
         sum(totalsales) as money_spent
          from `rmf analysis data set`.rfm_clean
          group by customerID 
          )
          SELECT * from loyal
          WHERE frequency > 10 
          and recency > 180 
          and money_spent > 1000;
          
          
          SELECT 
    invoiceNo, 
    COUNT(DISTINCT StockCode) AS items_in_basket,
    SUM(totalsales) AS basket_value
FROM rfm_clean
GROUP BY invoiceNo
HAVING items_in_basket > 10
ORDER BY items_in_basket DESC;
			
			
              
              
             
		
         
             
             
             
                   
                
                
             
             
             
             
              
           
           
           
           
         
         
         
         
         
            
          
        
           
           
          
         
        
                  
                  
                  
                    
                  
                    
                    
                   
                   
                    
                   
                    
                  
                  
                 
                 
                 
             
             
             
             
             
         
         
         
		
       
   
   
 
 
  
  


