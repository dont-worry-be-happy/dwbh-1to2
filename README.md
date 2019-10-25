# dwbh-1to2

Migrate data from DWBH DB v1 o v2.


## HOWTO:

1. Create v2 DB

   ```
   docker run -d -p 5432:5432 postgres:10-alpine

   psql -U postgres -h localhost
   create role dwbh with password 'dwbh' login;
   create database dwbh owner dwbh;
   \q

   cd ../dwbh-api
   gradlew run
   ```

2. Export v1 DB to .sql file.

   ```
   pg_dump dwbh -O -x > dwbh_v1.sql
   ```

3. Import v1 DB into the new PostgreSQL instance.

   ```
   psql -U postgres -h localhost
   create database dwbh_old owner dwbh;
   \q
   psql -U dwbh -h localhost dwbh_old < dwbh_v1.sql
   ```

4. Run the script:
   ```
   python dwbh_1to2.py
   ```
