# for auto test

# 定期拷贝数据

import threading
import sys_time
import shutil
import schedule


oldfileName = "data.txt"
oldfilePath = oldfileName
count = 0
copyPath = "test/"
times = 0

def hello(minute):
    copy_file()

    global timer
    heartBeat_interval = minute*60
    if(times < 3):
       timer = threading.Timer(heartBeat_interval, hello, [minute])
       timer.start()
       times = times + 1
    else:


def copy_file():
    global count
    newPath = copyPath + oldfileName + "." + str(count);
    newfile = open(newPath, 'w')
    newfile.close()

    shutil.copyfile(oldfilePath, newPath)
    print "copy file done: %s" % newPath
    count = count + 1

if __name__ == "__main__":
    schedule.every().day.at("19:58").do(job(1))

    while 1:
       schedule.run_pending()
       sys_time.sleep(1)
