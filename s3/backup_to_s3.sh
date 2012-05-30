#!/bin/bash
#
# File backup script to backup to remote server
#

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=`dirname "$SCRIPT"`

S3_CONF="$SCRIPTPATH/../conf/sample.cfg"

function usage
{
    echo "usage: ./$0 --source=/src/file/or/dir --bucketname=<bucketname> "
    exit 1
}

function log()
{
    echo "[`date +'%Y%m%d %H:%M:%S'`] $@"
}

while [ "$1" != "" ]; do
    param=${1%=*}
    value=${1#*=}
    case $param in
        --source)         source=$value
                           ;;
        --bucketname) bucketname=$value
                           ;;
        * )               usage
                           ;;
    esac
    shift
done

if [ "$source" == "" ]; then
    echo "Error: Missing --source"
    usage
fi

if [ ! -e $source ]; then
    log "Source: $source does not exist" 
fi

if [ "$bucketname" == "" ]; then
    echo "Error: Missing --bucketname"
    usage
fi

start_time=`date +%s`

log "copying to s3://${dest_dir}" 

files_to_copy=`find $source -type f -follow -print`
files_copied=0

for file in $files_to_copy; do
    $SCRIPTPATH/s3/s3_multipart_upload.py --conf_file=$S3_CONF --bucketname=$bucketname --backup_file=$file
    if [ "$?" == "0" ]; then
        log "backed up $file successfully"
        let "files_copied += 1"
    fi
done

end_time=`date +%s`
time_diff=`echo "$end_time - $start_time" | bc`


log "copy complete for $source, $files_copied files copied to S3, copy took $time_diff seconds" 
