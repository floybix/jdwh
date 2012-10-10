
create volatile table foo
as (
select
date'2010-12-31' as bar,
1.23456 as baz
) with data
on commit preserve rows;

select * from foo
union
select date'2000-01-01', 0.1 from foo
;
