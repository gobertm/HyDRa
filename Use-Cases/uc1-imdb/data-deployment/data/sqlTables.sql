create table mydb.directorTable
(
	id varchar(20) null,
	fullname varchar(70) null,
	birth int null,
	death int null,
	TEMP_ID int auto_increment
		primary key
);

create table mydb.directed
(
	director_id varchar(20) null,
	movie_id varchar(20) null
);