

select 
1.2345678,
-- test comment 1
'hello1'
;

/* the following fails with JDBC .executeQuery().
need to use the more general .execute() */

select 
1.2345678,
'hello2'
-- test comment 2
;
