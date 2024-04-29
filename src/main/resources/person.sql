create table person (
  id bigint primary key auto_increment,
  name varchar(255),
  age varchar(255),
  address varchar(255)
);

insert into person(name, age, address) values('한성민', '24', '서울');
insert into person(name, age, address) values('짱구', '5', '떡잎마을');
insert into person(name, age, address) values('상디', '20', '올블루');