create schema myproductdb collate utf8mb4_0900_ai_ci;

create table ProductCatalogTable
(
	product_id char(36) not null
		primary key,
	europrice char(36) null,
	description char(50) null
);

