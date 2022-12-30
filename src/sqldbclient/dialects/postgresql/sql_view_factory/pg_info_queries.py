from string import Template


PG_VIEWS_INFO_TEMPLATE = '''
    SELECT * 
    FROM pg_views 
    WHERE viewname = '{name}' AND schemaname = '{schema}'
'''

PG_MATVIEWS_INFO_TEMPLATE = '''
    SELECT * 
    FROM pg_matviews 
    WHERE matviewname = '{name}' AND schemaname = '{schema}';
'''

PG_OBJECT_DEPENDENCIES_TEMPLATE = '''
SELECT DISTINCT 
    dependent_ns.nspname as dependent_schema,
    dependent_view.relname as dependent_view,
    source_ns.nspname as source_schema,
    source_table.relname as source_table
FROM pg_depend 
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid 
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid 
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid 
JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid 
    AND pg_depend.refobjsubid = pg_attribute.attnum 
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE 
    source_ns.nspname = '{schema}'
    AND source_table.relname = '{name}'
    AND pg_attribute.attnum > 0 
ORDER BY 1,2
'''

PG_OBJECT_PRIVILEGES_TEMPLATE = Template('''
        SELECT 
            coalesce(nullif(s[1], ''), 'public') grantee, 
            (SELECT ARRAY_AGG(privilege ORDER BY privilege ASC)
                FROM (SELECT
                    CASE ch
                        WHEN 'r' THEN 'SELECT'
                        WHEN 'w' THEN 'UPDATE'
                        WHEN 'a' THEN 'INSERT'
                        WHEN 'd' THEN 'DELETE'
                        WHEN 'D' THEN 'TRUNCATE'
                        WHEN 'x' THEN 'REFERENCES'
                        WHEN 't' THEN 'TRIGGER'
                    END AS privilege
                    FROM regexp_split_to_table(s[2], '') ch
                ) s
            ) AS privileges
        FROM 
            pg_class
            JOIN pg_namespace ON pg_namespace.oid = relnamespace
            JOIN pg_roles ON pg_roles.oid = relowner,
            unnest(coalesce(relacl::text[], format('{%s=arwdDxt/%s}', rolname, rolname)::text[])) AS acl,
            regexp_split_to_array(acl, '=|/') AS s
        WHERE nspname = '$schema' and relname='$name'
''')

PG_OBJECT_INDEXES_TEMPLATE = '''
    SELECT 
        schemaname AS schema, 
        indexname AS name, 
        indexdef AS definition
    FROM pg_indexes
    WHERE tablename = '{name}' AND schemaname = '{schema}'
'''
