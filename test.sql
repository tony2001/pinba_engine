select * from request limit 10;
select * from timer limit 10;
select * from timertag limit 10;
select * from tag limit 20;

select * from request where id > 0 limit 1;
select * from timer where id > 0 limit 1;
select * from timertag where timer_id > 0 limit 1;
select * from timertag where tag_id > 0 limit 1;
select * from tag where id > 0 limit 1;

select * from request where id >= 0 limit 1;
select * from timer where id >= 0 limit 1;
select * from timertag where timer_id >= 0 limit 1;
select * from timertag where tag_id >= 0 limit 1;
select * from tag where id >= 0 limit 1;

select * from request where id in (0,1,2,100,1000) limit 1;
select * from timer where id in (0,1,2,100,1000) limit 1;
select * from timertag where timer_id in (0,1,2,100,1000) limit 1;
select * from timertag where tag_id in (0,1,2,100,1000) limit 1;
select * from tag where id in (0,1,2,100,1000) limit 1;

select * from request where script_name != "" order by script_name asc limit 2;
select * from request where hostname != "" order by hostname asc limit 2;
select * from request where req_time > 1 order by req_time asc limit 2;
select * from request where timers_cnt != 0 order by req_time desc limit 2;
select distinct(script_name) from request limit 10;
select distinct(hostname) from request limit 10;
select distinct(server_name) from request limit 10;

select * from timer where id = 0;
select * from timer where value > 0.3 order by value desc limit 2;
select * from timer where value between 0 and 0.5 order by value desc limit 2;
select * from timer where value not between 0 and 0.5 order by value desc limit 2;
select distinct(id) from timer limit 10;

select * from tag where id = 0;
select * from tag where id != 0 limit 1;
select * from tag where id < 1;
select * from tag where name != "" limit 2;
select * from tag where name like "gr%";
select distinct(name) from tag limit 10;

select * from timertag where timer_id = 0 limit 2;
select * from timertag where tag_id = 0 limit 2;
select * from timertag where tag_id = 0 limit 2;
select * from timertag where value != "" limit 3;
select * from timertag where timer_id < 100 and tag_id = 0 limit 10;
select distinct(value) from timertag;
