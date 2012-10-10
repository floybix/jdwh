
create volatile table foo
as (
select
date'2010-12-31' as bar,
1.23456 as baz
) with data
on commit preserve rows;

create table puserdata.jdwh_test
as (
select * from foo
union
select bar + 1, baz + 1 from foo
) with data;
