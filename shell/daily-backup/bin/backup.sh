#!/bin/bash

base=$(cd `dirname $0`; pwd)
source $base/env.sh

backupDir=$fileList/$yesDay

today=`date "+%Y%m%d"`
filter="back|_001|collection|tar|verf|$today"

backup(){
	currday=`date "+%Y-%m-%d %H:%M:%S"`
	echo "----------------------- backup time: $currday ---------------------"
	echo "file to filter: $filter"
	echo "backup data from $srcDataDir to $backupDir"

	mkdir -p $backupDir
	cp -rf `ls $srcDataDir/* | grep -E -v "$filter"` $backupDir/
	
	# get list snapshot
	_now=`date "+%Y%m%d_%H%M%S"`
	`ls -al $srcDataDir/ | grep -E -v "$filter" | grep "^-" > $snapshot/$_now`
    count=`ls -l $backupDir/ | grep "^-" | wc -l`
    yes_count=`ls -l $backupDir/ | grep "$yesDay" | wc -l`
    echo "yesterday - file count: $yes_count"
    echo "total - file count: $count"
	echo "---------------------------------- end -----------------------------------"
	echo ""
}

backup | tee -a $backupLog

