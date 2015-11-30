# sqoop import tested with netezza and using hcat to make use of orc encryption
# script copies template sqoop options file and replaces required variables in the new file
# it then kicks off the sqoop import utility with output written to a log file
# extra opt files  sqoop_var_init.opt sqoop_hive_init.hive sqoop_var_hcat_full.opt sqoop_var_hcat.opt
# first time run like (sqoop_import_hcat.sh TABLENAME CREATE SPLITBY)   empty hive orc table will be created
# to load one month run like (sqoop_import_hcat.sh TABLENAME 201501 SPLIITBY)
# to load full table run  like (sqoop_import_hcat.sh TABLENAME full SPLITBY)
