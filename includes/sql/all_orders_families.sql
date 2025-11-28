use promethius;

with RECURSIVE placed AS(
		# seed with all the accepted families
		SELECT n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, n.id as family_id
		FROM `names` as n 
		JOIN taxon_names as tn on tn.name_id = n.id
		JOIN taxa as t on t.taxon_name_id = tn.id
        where n.`rank` = 'family'
    UNION ALL
		# recurse up to the root but always copy the modifieds_id so we have a handle 
		SELECT n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, p.family_id as family_id
        FROM `names` as n 
		JOIN taxon_names as tn on tn.name_id = n.id
		JOIN taxa as t on t.taxon_name_id = tn.id # accepted names only
        JOIN placed as p on p.parent_id = t.id
        WHERE t.parent_id is not null AND t.parent_id != t.id
),

# convert placed into a list with the family and order filled in
placed_with_families AS(
	select m.id, m.name_alpha as family, o.name_alpha as 'order'  
	from placed as m 
	left join placed as o on m.id = o.family_id and o.`rank` = 'order'
	where m.family_id = m.id
)

select `order`, `family` from placed_with_families order by `order`, family;
