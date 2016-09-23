#!/bin/bash

#  daily-backup
#    |- transfer.log
#    |- files
#       |- 20160823
#       |- 20160824
#    |- snapshot
#    |- tarFiles

base=$(cd `dirname $0`; pwd)
source $base/env.sh

trans(){
    tarFile=$yesDay.tar.gz
    echo "------------ compress files for day: $yesDay ------------"
    fileDir=$fileList/$yesDay
	cd $fileDir 

    # make sure dir is not empty
    if [[ ! -d $fileDir || "`ls -A $fileDir`" = "" ]];then
        echo "!!!dir is not exist or empty, exit!!!"
        exit 0
    fi

    # tar file exist
    if [[ -f $tarFile ]]; then
        echo "file $tarFile exist, delete it and redo compress"
        rm -rf $tarFile
    fi

	tar acf $tarFile *
    if [ $?  -eq  0 ]; then
	    mv $tarFile $tarFilePath/
        echo "compress success, mv it to $tarFilePath"
        if [ $?  -eq  0 ]; then
            echo "mv success, clear files"
	        rm -rf $fileDir
        fi
    fi 
    echo ""
}

trans| tee -a $transLog

