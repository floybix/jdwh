

select
-- foo 'bar' baz epi   ; <- a bit tricky
'foo' -- there's more: ; <- quite tricky
,'bar'
,'baz'; --         and ; <- not too tricky

select 'ok';
