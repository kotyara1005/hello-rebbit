drop table if exists host;
drop table if exists host_type;
drop table if exists scan;
drop table if exists record_a;
drop table if exists record_cname;
drop table if exists record_mx;
drop table if exists record_ns;
drop table if exists record_soa;

create table host (
  id serial primary key,
  name text not null unique,
  type_id integer not null
);

create table host_type (
  id serial primary key,
  name text not null unique
);
insert into host_type (id, name) values (1, 'IP');
insert into host_type (id, name) values (2, 'DOMAIN');

create table scan (
  id serial primary key,
  host_id integer not null,
  query_a_status text not null,
  query_cname_status text not null,
  query_mx_status text not null,
  query_ns_status text not null,
  query_soa_status text not null,
  created_at text not null
);

create table record_a (
  id serial primary key,
  host_id integer not null,
  scan_id integer not null,
  ip_address_id integer not null,
  ttl integer
);

create table record_cname (
  id serial primary key,
  host_id integer not null,
  scan_id integer not null,
  cname text not null,
  ttl integer
);

create table record_mx (
  id serial primary key,
  host_id integer not null,
  scan_id integer not null,
  mail_id integer not null,
  priority integer,
  ttl integer
);

create table record_ns (
  id serial primary key,
  host_id integer not null,
  scan_id integer not null,
  nameserver_id integer not null,
  ttl integer
);

create table record_soa (
  id serial primary key,
  host_id integer not null,
  scan_id integer not null,
  nameserver_id integer not null,
  hostmaster text,
  serial integer,
  refresh integer,
  retry integer,
  expires integer,
  minttl integer,
  ttl integer
);

-- TODO add constrains