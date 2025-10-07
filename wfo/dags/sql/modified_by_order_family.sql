SET @days_since = 30;

use promethius;

with RECURSIVE placed AS(
		# seed with all the names that have been modified during the interval
		SELECT n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, n.id as modifieds_id
		FROM `names` as n 
		JOIN taxon_names as tn on tn.name_id = n.id
		JOIN taxa as t on t.id = tn.taxon_id
        where n.modified > DATE_SUB(NOW(), INTERVAL @days_since DAY)
    UNION ALL
		# recurse up to the root but always copy the modifieds_id so we have a handle 
		SELECT n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, p.modifieds_id as modifieds_id
        FROM `names` as n 
		JOIN taxon_names as tn on tn.name_id = n.id
		JOIN taxa as t on t.taxon_name_id = tn.id # accepted names only
        JOIN placed as p on p.parent_id = t.id
        WHERE t.parent_id is not null AND t.parent_id != t.id
),

# convert placed into a list with the family and order filled in
placed_with_families AS(
	select m.id, m.name_alpha, f.name_alpha as family, o.name_alpha as 'order'  
	from placed as m 
	left join placed as f on m.id = f.modifieds_id and f.`rank` = 'family'
	left join placed as o on m.id = o.modifieds_id and o.`rank` = 'order'
	where m.modifieds_id = m.id
),

# get a list of the unplaced with their putative genera
unplaced as (
        SELECT 
			n.id as name_id, 
            n.name_alpha,
			IF (n.`rank` = 'genus', n.`name`, n.genus) as genus_name # either is a genus or has a genus name
		FROM `names` as n 
		LEFT JOIN taxon_names as tn on n.id = tn.name_id
        where n.modified > DATE_SUB(NOW(), INTERVAL @days_since DAY) # has been modified
        AND tn.taxon_id is null # not placed in the classification
        AND (n.`rank` = 'genus' or n.genus is not null) # not interested if we can't get a genus name
),

# join it to genera that exist in the classification and have the same names
# as a seed for the same recursive climb
unplaced_placed AS(
	SELECT
		n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, u.name_id as modifieds_id
	FROM `names` AS n 
	JOIN taxon_names AS tn ON tn.name_id = n.id
	JOIN taxa AS t ON t.id = tn.taxon_id
    JOIN unplaced AS u ON u.genus_name = n.`name` AND n.`rank` = 'genus'

    UNION ALL
		# recurse up to the root but always copy the modifieds_id so we have a handle 
		SELECT n.id, n.name_alpha, n.`rank`, t.parent_id as parent_id, p.modifieds_id as modifieds_id
        FROM `names` as n 
		JOIN taxon_names as tn on tn.name_id = n.id
		JOIN taxa as t on t.taxon_name_id = tn.id # accepted names only
        JOIN unplaced_placed as p on p.parent_id = t.id
        WHERE t.parent_id is not null AND t.parent_id != t.id
	
),

# expand the unplaced with the family and genus names
unplaced_with_families AS(
	select u.name_id as id, u.name_alpha, f.name_alpha as family, o.name_alpha as 'order'  
    FROM unplaced as u
	left join unplaced_placed as f on u.name_id = f.modifieds_id and f.`rank` = 'family'
	left join unplaced_placed as o on u.name_id = o.modifieds_id and o.`rank` = 'order'
),

all_with_families as (
	SELECT * FROM placed_with_families
    UNION ALL
    SELECT * FROM unplaced_with_families
)

# finally expand it with the whole name record
SELECT i.`value` as wfo, u.`name` as user_name, u.orcid_id as user_orcid, n.*, f.`order`, f.family
FROM `names` AS n
JOIN all_with_families AS f ON n.id = f.id
JOIN identifiers as i on n.prescribed_id = i.id and i.kind = 'wfo'
JOIN users as u on n.user_id = u.id
ORDER BY `order`, `family`, modified desc