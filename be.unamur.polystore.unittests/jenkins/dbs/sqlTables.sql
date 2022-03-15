create table mydb.directorTable
(
	id varchar(20) primary key,
	fullname varchar(70) null,
	birth int null,
	death int null
);

create table mydb.directed
(
	director_id varchar(20) null,
	movie_id varchar(20) null
);

create table mydb.reviewTable
(
	id varchar(20) primary key,
	content varchar(180) null,
	movieid varchar(20) null
);

create table mydb.movieTable
(
	id varchar(20) primary key,
	title varchar(140) null,
	director_id varchar(20) null
);

