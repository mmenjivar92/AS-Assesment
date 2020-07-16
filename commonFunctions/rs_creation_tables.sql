/*To be executed in a redshift cluster*/
create table dim_aisle(
	aisle_key VARCHAR(100) PRIMARY KEY,
    aisle_name VARCHAR(500),
    inserted_date date
);

create table dim_department(
	department_key VARCHAR(100) PRIMARY KEY,
    department_name VARCHAR(500),
    inserted_date date
);

create table dim_product(
	product_key VARCHAR(100),
    aisle_key VARCHAR(100)
    product_name VARCHAR(500),
    inserted_date date
);

create table dim_user(
	user_key VARCHAR(100) PRIMARY KEY,
    inserted_date date
);

create table fact_order_items(
	order_key VARCHAR(500),
    user_key VARCHAR(500),
    order_number VARCHAR(500),
    day_key VARCHAR(500),
    hour_key VARCHAR(500),
    aisle_key VARCHAR(500),
    department_key VARCHAR(500)
);