#! /bin/bash

baseDir=$(cd `dirname $0`; pwd|grep -o -E ".+[^/bin]")

srcDataDir=/opt/flume/src-data

# trans log file
transLog=$baseDir/transfer.log
backupLog=$baseDir/backupLog.log

# backup data path
fileList=$baseDir/fileList

# backup data listpath
snapshot=$baseDir/snapshot

# ´ò¿¼
tarFilePath=$baseDir/zips

yesDay=`date -d 'yesterday' "+%Y%m%d"`

mkdir -p $fileList
mkdir -p $snapshot
mkdir -p $tarFilePath
