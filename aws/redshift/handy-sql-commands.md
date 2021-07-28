- NOTE: SQL workbench can sometimes be locked in 'hang' status (showing 'executing statement')- Always check redshift 'load' console to see if a table really loaded

- Get columns names:
```
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Your Table Name'
ORDER BY ORDINAL_POSITION
```


- Checking duplicate rows:
```
# https://chartio.com/learn/databases/how-to-find-duplicate-values-in-a-sql-table/
SELECT username, email, COUNT(*)
FROM users
GROUP BY username, email
HAVING COUNT(*) > 1
```
```
# List all rows containing duplicates
SELECT a.*
FROM users a
JOIN (SELECT username, email, COUNT(*)
FROM users 
GROUP BY username, email
HAVING count(*) > 1 ) b
ON a.username = b.username
AND a.email = b.email
ORDER BY a.email
```

- Check stl_load_commits table to see the last commit:
```
select query, trim(filename) as file, curtime as updated
from stl_load_commits order by updated desc;
```

- Check data distribution on all tables:
```
select slice, col, num_values, minvalue, maxvalue
from svv_diskusage
where name='customer' and col=0
order by slice,col;
```

- Look fir disk spills:
```
select query, step, rows, workmem, label, is_diskbased
from svl_query_summary
where query = [YOUR-QUERY-ID]
order by workmem desc;
```

- Check column, distkey, sortkey for a given table:
```
select "column", type, encoding, distkey, sortkey, "notnull" 
from pg_table_def
where tablename = 'lineorder';
```

- To list all tables:
```
SELECT * FROM information_schema.tables;
```

- To list tables in public schema:
```
SELECT table_name FROM information_schema.tables WHERE table_schema='public'
```

### Athena
- Connection with SQL Workbench:
```
# https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.5/docs/Simba+Athena+JDBC+Driver+Install+and+Configuration+Guide.pdf
# Building the Connection URL

jdbc:awsathena://AwsRegion=[Region];UID=[AccessKey];PWD=
[SecretKey];S3OutputLocation=[Output];[Property1]=[Value1];
[Property2]=[Value2];...
```

- Create an axternal table in Athena:
```
CREATE TABLE table_name
[ WITH ( property_name = expression [, ...] ) ]
AS query
[ WITH [ NO ] DATA ]

# example
CREATE TABLE DB_NAME.table_name
WITH (
  format='TEXTFILE'
) AS
SELECT * FROM select distinct(createdat) from "DB"."layer2sessions";
```
