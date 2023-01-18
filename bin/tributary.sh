#!/bin/bash

# shellcheck disable=SC2046
# shellcheck disable=SC2164
base_path=$(cd $(dirname "$0");cd ../; pwd)
cd "${base_path}"

stop() {
  ps -ef | grep tributary |grep -v grep | awk '{print $2}' | xargs -I {} kill {}
  while true
  do
    # shellcheck disable=SC2009
    pid=$(ps -ef | grep tributary |grep -v grep | awk '{print $2}')
    if [ -z "${pid}" ]
    then
      break
    fi
    format_pid=$(echo "$pid"| tr "\n" " ")
    echo "tributary is doing graceful shutdown, pid =${format_pid}, sleep 2 second and check it again"
    sleep 2
  done
}

# shellcheck disable=SC2231
start() {
  home=${TRIBUTARY_HOME-"${base_path}"}
  conf=${TRIBUTARY_CONF-"${home}/config"}
  log=${TRIBUTARY_LOG-"${home}/log"}
  export TRIBUTARY_LOG=$log
  CLASSPATH=${conf}
  for f in ${home}/libs/*.jar
  do
    CLASSPATH=${CLASSPATH}:${f}
  done
  for f in ${home}/*.jar
  do
    CLASSPATH=${CLASSPATH}:${f}
  done
  export CLASSPATH
  nohup java -Xms1G -Xmx1G org.zicat.tributary.service.TributaryServiceApplication > /dev/null 2>&1 &
}

restart() {
  stop
  start
}

action=$1
if [ "$action" = "start" ]; then
  start
elif [ "$action" = "stop" ]; then
  stop
elif [ "$action" = "restart" ]; then
  restart
else
  echo "action ${action} not support"
fi
