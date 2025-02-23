INSERT INTO public.entities(name, entity_type)
	SELECT DISTINCT name, entity_type from public.staging_entity_counts
	on CONFLICT (name) DO NOTHING;

INSERT INTO public.entity_counts (entity_id, count, date)
select
	e.id as entity_id,
	s.count,
	s.date
from public.staging_entity_counts s
join public.entities e on s.name = e.name;
