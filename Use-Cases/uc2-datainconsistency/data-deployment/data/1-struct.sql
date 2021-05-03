
create table if not exists ProductCatalogTable
(
	product_id char(36) not null
		primary key,
	dollarprice char(36) null,
	description char(50) null
);

