#!/bin/bash
export PATH=/etc:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:/usr/java/jdk1.8.0_162/bin
export JAVA_HOME=/usr/java/jdk1.8.0_162

function start_dd() {
    nohup java -jar /home/pengshan/jar/dtc-dd.jar >> /home/pengshan/jar/logs/dd-data.log 2>&1 &
}
function stop_dd() {
    PID=$(cat /var/run/dtc-dd.pid)
    kill -9 $PID
}

case $1 in
"start_dd"){
        start_dd
};;
"stop_dd"){
        stop_dd
};;
esac