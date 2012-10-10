
select *
from padm.star_thresholds
order by model;

select model, 1 as "flag"
from padm.star_thresholds
order by model desc;
