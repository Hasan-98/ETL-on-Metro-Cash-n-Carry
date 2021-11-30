





#drop schema if exists MEtro_dw;
#create schema METRO_DW;

create table product_dim_table
(product_id varchar(6) primary key,
product_name varchar(30),
unit_price decimal(5,2)
);

create table supplier_dim_table
(supplier_id varchar(5) primary key,
supplier_name varchar(30)
);

create table customer_dim_table
(customer_id varchar(6) primary key,
customer_name varchar(30)
);

create table store_dim_table
(store_id varchar(4) primary key,
store_name varchar(20)
);

create table time_dim_table
(Time_ID date primary key,
day int,
month int,
quarter varchar(10),
year int
);

create table Salesfact_table
(Customer_id varchar(4),
Product_ID varchar(6),
supplier_id varchar(5),
store_id varchar(4),
time_id date,
quantity smallint,
totalprice decimal (10,4),
primary key (Customer_id,Product_ID,supplier_id,store_id,time_id),
foreign key (product_id) references product_dim_table(product_id),
foreign key (customer_id) references customer_dim_table(customer_id),
foreign key (supplier_id) references supplier_dim_table(supplier_id),
foreign key (store_id) references store_dim_table(store_id), 
foreign key (time_id) references time_dim_table(time_id)
);


select * from product_dim_table;
select * from supplier_dim_table;
select * from customer_dim_table;
select * from store_dim_table;
select * from time_dim_table;
select  count(*) from Salesfact_table;