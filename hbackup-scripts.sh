#!/bin/bash

CURRENTTIME="$(date +'%Y%m%d%H%M')"
DATE="$(date +'%Y%m%d')"
TABLE_NAME=$1
EXPORT_CMD="sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.Export"

exportHbaseTables()
{

    echo "Backing up Hbase Tables ...."

    $EXPORT_CMD "$1" $2/"$1"_$3

    hadoop fs -ls $2/"$1_$3"

    echo "Backup Location on HDFS : hadoop fs -ls $2/"

    echo "Backup Complete ..."
}

dumptables() {

    echo "Dumping HDFS backup file to local filesystem..."

    hadoop fs -copyToLocal "$1" "$2"

}

backupType=$2

if [ -z "$backupType" ]; then
    echo -e "For a Complete backup, use the below command:"
    echo -e "usage: sh filename.sh TBL_NAME FULL_BACKUP \n"
    echo -e "For an Incremental backup, use the below command - it will take the last 4 hrs backup:"
    echo -e "usage: sh filename.sh TBL_NAME INCREMENTAL_BACKUP\n"
    exit 1
fi

# Setting Basepath for HDFS Location
BASE_PATH="/apps/hbase"

if [ "$backupType" == "FULL_BACKUP" ]; then
    # Complete backup for all versions
    echo -e "HBASE BACKUP: Starting FULL_BACKUP\n"
    backupStartTimestamp="-2147483648"
    backupEndTimestamp="$(date +%s)000"
    versionNumber="2147483648"
    
    # Creating backup Base Path
    BACKUP_BASE_PATH="$BASE_PATH/$backupType"

    echo -e "Starting FULL BACKUP - data will be stored in $BACKUP_BASE_PATH/"

    exportHbaseTables "$TABLE_NAME" "$BACKUP_BASE_PATH" "$CURRENTTIME" "$versionNumber" "$backupStartTimestamp" "$backupEndTimestamp"

    # Dump HDFS to local
    dumptables "$BACKUP_BASE_PATH/"$TABLE_NAME"_$CURRENTTIME" "/hbase-backup/FULL"


elif [ "$backupType" == "INCREMENTAL_BACKUP" ]; then

    # Setting a parameter & it will take incremental backup from the last 4 hours till now

    echo -e "HBASE BACKUP: Starting INCREMENTAL_BACKUP\n"
    backupStartTimestamp="$(date --date='4 hours ago' +%s)000"
    backupEndTimestamp="$(date +%s)000"
    versionNumber="2147483647"

    BACKUP_BASE_PATH="$BASE_PATH/$backupType"

    echo -e "Starting INCREMENTAL BACKUP - data will be stored in $BACKUP_BASE_PATH/"

    exportHbaseTables "$TABLE_NAME" "$BACKUP_BASE_PATH" "$CURRENTTIME" "$versionNumber" "$backupStartTimestamp" "$backupEndTimestamp"

    # Dump HDFS to local
    dumptables "$BACKUP_BASE_PATH/"$TABLE_NAME"_$CURRENTTIME" "/hbase-backup/INCREMENTAL"
else
    echo -e "Enter Correct Parameter \n"
    exit 1
fi
