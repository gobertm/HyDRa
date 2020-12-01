create database mydb;
use mydb;

create table mydb.CreditCard (
	id char(36) not null unique,
	expiryDate date,
	number text,
	userid char(36) not null,
	primary key (id)
);

create table mydb.User (
	id char(36) not null unique,
	name text,
	primary key (id)
);

create table mydb.Orders (
id char(36) not null unique,
orderDate date,
totalAmount int,
creditcardid char(36),
userid char(36) not null,
primary key (id)
);

create table mydb.Product (
	id char(36) not null unique,
	name text,
	description text,
	price float,
	primary key (id)
);

create table mydb.Order_Product (
productid char(36) not null,
orderid char(36) not null,
foreign key(productid) references Product(id) on delete cascade,
foreign key(orderid) references mydb.Orders(id) on delete cascade
);

alter table mydb.CreditCard add constraint foreign key (userid) references User(id);
alter table mydb.Orders add constraint foreign key (userid) references User(id);
alter table mydb.Orders add constraint foreign key (creditcardid) references CreditCard(id);




/* User */
insert into mydb.User VALUES ('technicaluserid124','Maxime Gobert');
insert into mydb.User VALUES ('technicaluserid432','Anthony Cleve');
insert into mydb.User VALUES ('technicaluserid987','Mr X.');


/* CreditCard */

insert into mydb.CreditCard VALUES ('technicalccid123456',str_to_date('01122021','%d%m%Y'),'53984578695211','technicaluserid124');
insert into mydb.CreditCard VALUES ('technicalccid456785',str_to_date('01012022','%d%m%Y'),'69854714523633','technicaluserid124');
insert into mydb.CreditCard VALUES ('technicalccid784569',str_to_date('01082020','%d%m%Y'),'45611454245132','technicaluserid432');

/* Order */
insert into mydb.Orders VALUES ('technicalorderid12072019',str_to_date('12072019','%d%m%Y'),50,'technicalccid123456','technicaluserid124');
insert into mydb.Orders VALUES ('technicalorderid20062019',str_to_date('20062019','%d%m%Y'),12,'technicalccid123456','technicaluserid124');
insert into mydb.Orders VALUES ('technicalorderid03062019',str_to_date('03062019','%d%m%Y'), 32, 'technicalccid784569', 'technicaluserid124');
insert into mydb.Orders VALUES ('technicalorderid05052019',str_to_date('05052019','%d%m%Y'),120, null, 'technicaluserid987');

/* Product */

insert into mydb.Product VALUES ('technicalproductid455454','Shampoing','Ca lave les cheveux',3.26);
insert into mydb.Product VALUES ('technicalproductid656565','Jupiler','De la pils correcte',0.52);
insert into mydb.Product VALUES ('technicalproductid121212','Galer','Du chocolat',1.25);
insert into mydb.Product VALUES ('technicalproductid323232', 'Evian', 'Eau de source', 2);

/*Product_Orders */

insert into mydb.Order_Product VALUES ('technicalproductid121212', 'technicalorderid03062019');
insert into mydb.Order_Product VALUES ('technicalproductid656565', 'technicalorderid03062019');
insert into mydb.Order_Product VALUES ('technicalproductid656565', 'technicalorderid05052019');
insert into mydb.Order_Product VALUES ('technicalproductid323232', 'technicalorderid20062019');
insert into mydb.Order_Product VALUES ('technicalproductid455454', 'technicalorderid20062019');
