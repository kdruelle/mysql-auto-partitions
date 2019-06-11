# MySQL Auto Partitions

**Mysql Auto Partition** is a script that aims to automatically manage range partitionned tables by date and purge old partitions. It use the Event scheduler to trigger procedure calls every hour.

## Prerequist
 - The Event Scheduler shall be activated [checkout documentation](https://dev.mysql.com/doc/refman/5.7/en/events-configuration.html)
 - The date field of the table shall be contained in primary key

## How to use this script

Execute the **partitions.sql** script on your database. This will define the procedures and create the **manage_partition** table allowing define your retention policy.

|Field            |Signification                        | Exemple    |
|-----------------------|-------------------------------------------------------|---------------|
|tablename        |The name of the table you want to partition        | test_table    |
|table_date_field    |Name of the column holding the partitionning field    | date        |
|period            |Either 'daily' or 'monthly'                | daily        |
|future            |How many future partition to create            | 3        |
|keep_history        |How many days or to keep partitions (depend on **period**)| 30    |
|last_updated        |Internaly used                        |        |
|comments        |Whatever you whant                    | Testing table |

Then for initialize a new table you just have to do the following :
```sql
INSERT INTO `manage_partitions` VALUES('test_table', 'date', 'daily', 3, 30, NULL, 'Testing table');
CALL `init_partitons`();
```

## Licence
Apache License, Version 2.0
