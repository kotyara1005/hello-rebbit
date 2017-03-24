drop table if exists records;
create table records (
  id integer primary key autoincrement,
  host text not null unique,
  ip text,
  last_refresh real not null
);