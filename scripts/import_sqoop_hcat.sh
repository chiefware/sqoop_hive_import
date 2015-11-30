#!/bin/bash

# script copies template sqoop options file and replaces required variables in the new file
# it then kicks off the sqoop import utility with output written to a log file
# extra opt files  sqoop_var_init.opt sqoop_hive_init.hive sqoop_var_hcat_full.opt sqoop_var_hcat.opt
# first time run like (sqoop_import_hcat.sh CHIEFTABLE CREATE CHIEFSPLITBY)  empty hive orc table will be created
# to load one month run like (sqoop_import_hcat.sh CHIEFTABLE 201501 CHIEFSPLITBY)
# to load full table run  like (sqoop_import_hcat.sh CHIEFTABLE full CHIEFSPLITBY)

TABLE=$1
PERIOD=$2
SPLITBY=$3

init()
{
  ERROR=1
  GOOD=0
  unset HADOOP_MAPRED_HOME
  db_name=P_ORI_W
  LOGDIR=/log/sqoop/
  OPTDIR=$OPTDIR


  datum0=$PERIOD
  datum1="$PERIOD""00"
  datum2="$PERIOD""99"
  LOG="$TABLE"_"$datum0"
  logfile="$LOGDIR""$LOG"$(date +"%Y%m%d%H%M")

  # to upper
  TABLE=$(echo ${TABLE} | tr 'a-z' 'A-Z')
  TABLEL=$(echo ${TABLE} | tr 'A-Z' 'a-z')
  SPLITBY=$(echo ${SPLITBY} | tr 'a-z' 'A-Z')
}

quit() {
  exit $1 
}

error() {
  echo "You need to input table_name date or full (e.g. sqoop_import.sh CHIEFTABLE 201506 CHIEFSPLITBY  or sqoop_import.sh DSEG full SEGID)"
  echo 
  quit $ERROR
}

sqoop_it()
{

  if [ ! -z $TABLE ] && [ ! -z $PERIOD ];
  then
     if [ $PERIOD = "create" ];
     then
	read -r -p "This step creates table $TABLE in ORC format and deletes all $TABLE hdfs files are you sure? [Y/N] " response
	response=${response,,}    # tolower
	if [[ $response =~ ^(yes|y)$ ]]
	then 
	 echo "lets go"
	else
	 echo "script aborted"
	 exit
	fi 
        echo "Create Hive table $TABLE $PERIOD at " >> "$logfile" 2>&1
        date >> "$logfile"
        echo "remove file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        rm $OPTDIR/sqoop_$TABLE.opt 
        sleep 1
        echo "copy to file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        cp $OPTDIR/eeci-warehouse/scripts/sqoop_var_init.opt $OPTDIR/sqoop_$TABLE.opt 
        sleep 1
        	sed -i -e "s/db_name/$db_name/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change db_name to $db_name in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        	sed -i -e "s/table_name/$TABLE/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change table_name to $TABLE in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        	sed -i -e "s/split_name/$SPLITBY/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change split_name to $SPLITBY in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        CMD="/usr/bin/sqoop import --options-file $OPTDIR/sqoop_$TABLE.opt"
        echo $CMD
        $CMD  >> "$logfile" 2>&1
        echo "Ending Sqoop import  $TABLE $PERIOD at "  >> "$logfile" 2>&1
	echo "Starting alter hive $TABLE to ORC "  >> "$logfile" 2>&1
        rm $OPTDIR/sqoop_hive_init_$TABLE.hive
	sleep 1
        cp $OPTDIR/eeci-warehouse/scripts/sqoop_hive_init.hive $OPTDIR/sqoop_hive_init_$TABLE.hive
        	sed -i -e "s/table_name/$TABLE/g" $OPTDIR/sqoop_hive_init_$TABLE.hive
        echo "change table_name to $TABLE in file $OPTDIR/sqoop_hive_init_$TABLE.hive" >> "$logfile" 2>&1
	sleep 1
	hive -f "$OPTDIR/sqoop_hive_init_$TABLE.hive" >> "$logfile" 2>&1
	echo "Ending alter hive $TABLE to ORC "  >> "$logfile" 2>&1
	echo "Starting remove hadoop files from hive $TABLE to ORC "  >> "$logfile" 2>&1
	echo "Ending remove hadoop files from hive $TABLE to ORC "  >> "$logfile" 2>&1
     elif [ $PERIOD = "full" ];
     then 
        echo "Starting Sqoop import $TABLE $PERIOD at " >> "$logfile" 2>&1
        date >> "$logfile"
        rm $OPTDIR/sqoop_$TABLE.opt 
        sleep 1
        echo "remove file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        cp $OPTDIR/eeci-warehouse/scripts/sqoop_var_hcat_full.opt $OPTDIR/sqoop_$TABLE.opt 
        sleep 1
        echo "copy to file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        #sed "s/db_name/$db_name/g" $OPTDIR/sqoop_$TABLE.opt | tee $OPTDIR/sqoop_$TABLE.opt 
        	sed -i -e "s/db_name/$db_name/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change db_name to $db_name in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        	sed -i -e "s/table_name/$TABLE/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change table_name to $TABLE in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        	sed -i -e "s/split_name/$SPLITBY/g" $OPTDIR/sqoop_$TABLE.opt
        echo "change split_name to $SPLITBY in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
        CMD="/usr/bin/sqoop import --options-file $OPTDIR/sqoop_$TABLE.opt"
        echo $CMD
        $CMD  >> "$logfile" 2>&1
        echo "Ending Sqoop import  $TABLE $PERIOD at "  >> "$logfile" 2>&1
        date >> "$logfile"
     else
       echo "Starting Sqoop import $TABLE $PERIOD at " >> "$logfile" 2>&1
       date >> "$logfile"
       rm $OPTDIR/sqoop_$TABLE.opt
       sleep 1
       echo "remove file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       cp $OPTDIR/eeci-warehouse/scripts/sqoop_var_hcat.opt $OPTDIR/sqoop_$TABLE.opt
       sleep 1
       echo "copy to file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       		sed -i -e "s/db_name/$db_name/g" $OPTDIR/sqoop_$TABLE.opt
       echo "change $dbname in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       		sed -i -e "s/table_name/$TABLE/g" $OPTDIR/sqoop_$TABLE.opt
       echo "change $TABLE in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       		sed -i -e "s/datum1/$datum1/g" $OPTDIR/sqoop_$TABLE.opt
       echo "change $datum1 in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       		sed -i -e "s/datum2/$datum2/g" $OPTDIR/sqoop_$TABLE.opt
       echo "change $datum2 in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       		sed -i -e "s/split_name/$SPLITBY/g" $OPTDIR/sqoop_$TABLE.opt
       echo "change $SPLITBY in file $OPTDIR/sqoop_$TABLE.opt" >> "$logfile" 2>&1
       CMD="/usr/bin/sqoop import --options-file $OPTDIR/sqoop_$TABLE.opt" 
       echo $CMD
       $CMD  >> "$logfile" 2>&1
       echo "Ending Sqoop import  $TABLE $PERIOD at "  >> "$logfile" 2>&1
       date >> "$logfile"
     fi
  else
     error
  fi
}

cleanup_hadoop()
{
     if [ ! -z $TABLEL ] && `hdfs dfs -test -d /data/raw_sqoop/$TABLE`
     then
     	export SQOOP_SERVICE=1
     	/usr/bin/hadoop fs -rm -r /data/raw_sqoop/$TABLE   >> "$logfile" 2>&1
     else 
     	echo "No Hadoop files deleted" >> "$logfile" 2>&1
     fi
}

cleanup_hadoop_db()
{
     if [ ! -z $TABLEL ] && `hdfs dfs -test -d /apps/hive/warehouse/raw_sqoop.db/$TABLEL`
     then
     	export SQOOP_SERVICE=1
     	/usr/bin/hadoop fs -rm  /apps/hive/warehouse/raw_sqoop.db/$TABLEL/*   >> "$logfile" 2>&1
     else 
     	echo "No Hadoop files deleted" >> "$logfile" 2>&1
     fi
}

error_handling()
{
   error_count=$(egrep -w 'FAILED|error|ERROR|Error|required' $logfile | wc -l)
   if [[ $error_count > 0  ]];
   then
   	echo "script aborted check logfile $logfile for errors" >&2
   	echo "script aborted check logfile $logfile for errors"
   	echo "failed" >> "$logfile"
   	exit 1
   else 
        echo "$jobstep,success,$starttime,$endtime,$diff"  >> "$logfile"
   fi
}

# MAIN
init
if [ $PERIOD = "create" ];
then 
   cleanup_hadoop
else 
   echo "no cleanup"
fi 
sqoop_it
if [ $PERIOD = "create" ];
then 
   echo "Starting remove hadoop files from hive $TABLE to ORC "  >> "$logfile" 2>&1
   cleanup_hadoop_db
   echo "Ending remove hadoop files from hive $TABLE to ORC "  >> "$logfile" 2>&1
else 
 echo "no cleanup db"
fi
error_handling
exit 0
