# B2HANDLE-Migration
Helper tool to migrate EUDAT PID records for B2SAFE records of the EUDAT project

The script is run on a node with a handle database installed

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




