create table orderTable 
(
	orderId varchar(80),
	orderDate date null,
	totalamount double null,
	customerId varchar(80) null,
	PRIMARY KEY(orderId)
);

create table orderTableA
(
	orderId varchar(80), 
	orderDate date null,
	customerId varchar(80) null,
	PRIMARY KEY(orderId)
);

create table orderTableB
(
	id varchar(80), 
	orderAmount double null,
	PRIMARY KEY(id)
);

create table productTable
(
	asin varchar(20),
	title text,
	price float,
	imgUrl varchar(200),
	PRIMARY KEY(asin)
);

create table detailOrderTable(
	order_id varchar(80),
	product_id varchar(20)
);

create table customerTable
(
	id varchar(20),
	firstName varchar(50) null,
	lastName varchar(50) null,
	gender varchar(20) null,
	birthday date null,
	creationDate date null,
	locationIP varchar(30) null,
	browserUsed varchar(20) null,
	place int null,
	PRIMARY KEY(id)
);



