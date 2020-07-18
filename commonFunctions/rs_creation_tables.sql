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
    aisle_key VARCHAR(100) DISTKEY,
    product_name VARCHAR(500),
    inserted_date date,
    PRIMARY KEY (product_key,aisle_key),
    FOREIGN KEY(aisle_key) REFERENCES dim_aisle(aisle_key)
);

create table dim_user(
	user_key VARCHAR(100) PRIMARY KEY,
    inserted_date date
);

create table dim_day(
    day_key varchar(500) PRIMARY KEY,
    short_name varchar(15),
    long_name varchar(20)
);

insert into dim_day(day_key, short_name, long_name) VALUES ('0','Su','Sunday');
insert into dim_day(day_key, short_name, long_name) VALUES ('1','Mo','Monday');
insert into dim_day(day_key, short_name, long_name) VALUES ('2','Tu','Tuesday');
insert into dim_day(day_key, short_name, long_name) VALUES ('3','We','Wednesday');
insert into dim_day(day_key, short_name, long_name) VALUES ('4','Th','Thursday');
insert into dim_day(day_key, short_name, long_name) VALUES ('5','Fr','Friday');
insert into dim_day(day_key, short_name, long_name) VALUES ('6','Sa','Saturday');

create table dim_hour(
    hour_key varchar(500) PRIMARY KEY,
    twelve_hour_value varchar(15),
    twentyfour_hour_value varchar(15),
    period_of_day varchar(20)
);

insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('00','0am','0','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('01','1am','1','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('02','2am','2','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('03','3am','3','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('04','4am','4','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('05','5am','5','Early morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('06','6am','6','Early morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('07','7am','7','Early morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('08','8am','8','Early morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('09','9am','9','Morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('10','10am','10','Morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('11','11am','11','Late morning');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('12','12pm','12','Afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('13','1pm','13','Early afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('14','2pm','14','Early afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('15','3pm','15','Early afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('16','4pm','16','Late afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('17','5pm','17','Late afternoon');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('18','6pm','18','Early evening');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('19','7pm','19','Early evening');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('20','8pm','20','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('21','9pm','21','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('22','10pm','22','Night');
insert into dim_hour(hour_key, twelve_hour_value, twentyfour_hour_value, period_of_day) VALUES ('23','11pm','23','Night');


create table fact_order_items(
	order_key VARCHAR(500),
    user_key VARCHAR(500),
    order_number VARCHAR(500),
    day_key VARCHAR(500),
    hour_key VARCHAR(500),
    aisle_key VARCHAR(500),
    department_key VARCHAR(500),
    product_key VARCHAR(500),
    PRIMARY KEY (order_key,aisle_key,product_key),
    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    FOREIGN KEY (day_key) REFERENCES dim_day(day_key),
    FOREIGN KEY (hour_key) REFERENCES dim_hour(hour_key),
    FOREIGN KEY (aisle_key) REFERENCES dim_aisle(aisle_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (product_key,aisle_key) REFERENCES dim_product(product_key,aisle_key)
                             )
    DISTKEY(aisle_key)
;

create table dim_order_product_bridge(
	order_key VARCHAR(500),
    product_key VARCHAR(500),
    aisle_key VARCHAR(100) DISTKEY,
    add_to_cart_order INTEGER,
    PRIMARY KEY (order_key,product_key),
    FOREIGN KEY (order_key,product_key,aisle_key) REFERENCES fact_order_items(order_key,product_key,aisle_key),
    FOREIGN KEY (product_key,aisle_key) REFERENCES dim_product(product_key,aisle_key)
);
