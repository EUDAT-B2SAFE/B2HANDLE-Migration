# B2HANDLE-Migration

In this repo, there are two migration tools for different use cases:

* B2HANDLE-Migration (2017)
* SeaDataCloud-Migration (2021)


# SeaDataCloud Migration (2021)

This tool was used to migrate a number of PID records from one prefix to another.

It was needed because during the SeaDataCloud, the first ingested files had their PIDs created in the test prefix as the config was switched from the test credentials to the production credentials a little too late.

So we had to:

* Make new PID records in the production prefix
* Copy the contents from the test prefix to the production prefix
* Add explanatory notes to the old and new records 
* Make a redirection from the old to the next records (which will only work with browsers who follow redirects, not for machines).

These things are done by the `pid_migration.py` (done March 2021). There is another script `store_previous_pids.py` to make a backup of the records before the migration.

Also, what has to be done too:

* Make sure the customer changes the PID strings in their database
* Change the stored PIDs in the B2SAFE metadata
* Eventually, delete the values from the old PIDs (except for the redirection and the explanatory notes), so we avoid redundancy. Any client relying on these values will notice that they have to wrong/old PIDs. If we left the values in there, they might be used, and the client might not notice they're using an old record, while values in the new record might have changed.

Merret Buurman, DKRZ, 2021-04-08.



# B2HANDLE-Migration (2017)

Helper tool to migrate EUDAT PID records for B2SAFE records of the EUDAT project

The script is run on a node with a handle database installed

The script will only migrate handles which have a field with the type `CHECKSUM` in them and do not have `EUDAT/PID_PROFILE` in them. This has been chosen as a way to detect for B2SAFE handles which have not been migrated.

As a prerequistite you need to install the latest B2HANDLE library with all it's dependancies

There are 2 ways to run the script:
* automated from begin to start
* parrellized to increase performance

## automatated conversion

* define a password for the database to access
```
PASSWORD=password
```
* run migration script to generate handle generic batch file
```
./migrationtool.py <handle_authentication_user> <database_host> <database_user> ${PASSWORD} <handle_database> <output_handle_batch> <prefix_to_process> --no-fixedcontent --handlekeyfile <handle_authentication_file>
```
An example is:
```
./migrationtool.py 841/USER01 localhost handle ${PASSWORD} mirror_841 ~/tmp/841/test_migrate_841.hdlbatch 841 --no-fixedcontent --handlekeyfile ~/etc/841/841_USER01_300_privkey.bin
```
* execute migration script to update handles
```
hdl-generic-batch <output_handle_batch> output_handle_batch.log>
```

## parrallelized conversion

* define a password for the database to access
```
PASSWORD=password
```
* create a file with all handles
```
 time mysql --user=<database_user> --password=${PASSWORD} <handle_database> -e "SELECT distinct handle FROM handles where handle like '<prefix>/%';" > handles_prefix_<prefix>
```
* divide the handle file in parts with 500.000 lines each
```
split -l 500000 handles_prefix_<prefix> handles_prefix_<prefix>_
```
* run migration script to generate handle generic batch file
```
./migrationtool.py <handle_authentication_user> <database_host> <database_user> ${PASSWORD} <handle_database> <output_handle_batch_ax> <prefix_to_process> --no-fixedcontent --handlekeyfile <handle_authentication_file> --inputfile handles_prefix_<prefix>_ax
```
An example is:
```
./migrationtool.py 841/USER01 localhost handle ${PASSWORD} mirror_841 ~/tmp/841/test_migrate_841_a.hdlbatch 841 --no-fixedcontent --handlekeyfile ~/etc/841/841_USER01_300_privkey.bin --inputfile handles_prefix_841_aa
```
* execute migration script to update handles
```
hdl-generic-batch <output_handle_batch> output_handle_batch.log>
```

## Migration performance

Some performance tests have been done. It differs for master and child handles. With child handles it tries to go up the link if PPID is defined in the handle. A conversion of master handles is very fast. The process to create the handle batch file was timed as follows:
* master handles
500000 handles in about 40 minutes with 4 parrellel migrations running. So about 2 million in 40 minutes.
* child handles
500000 handles in about 4 hours with 6 parrellel migrations running. So about 3 million in 4 hours.

The process to really update the handles depends on the machine where the handles are residing. The process to run the handle batch file was timed as follows:
* 150 actions per seconds. About 3 actions per handle. About 50 handles updated per second. About 180.000 per hour. About 8 million handles where updated in 47 hour.


## error messages
The script generates messages:
* Warning: Broken PPID value in Handle `<prefix>/<suffix>`
* Handle `<prefix>/<suffix>` contains a PPID entry pointing to a non-existing Handle!
* Warning: Broken 10320/LOC record on `<prefix>/<suffix>`: href on id 0 does not match Handle's URL value!
* Warning: Bad Handle syntax for Handle `'<handle>'`!




