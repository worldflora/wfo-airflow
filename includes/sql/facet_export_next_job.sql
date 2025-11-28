SELECT s.id as data_source_id, j.export_format, j.include_synonyms, s.name as data_source_name 
FROM wfo_facets.export_jobs as j
JOIN wfo_facets.`sources` as s on j.source_id = s.id
LEFT JOIN `wfo_facets`.`snippet_sources` as ss on ss.source_id = s.id
where j.`status` < 1 
ORDER BY j.`created` asc
LIMIT 1;
