#!/bin/bash
source ~/.bash_profile

#函数定义
function stop_serv(){
    cnt=`ps -ef | grep "python36 rest.py" | grep -v grep | wc -l`
    if [ $cnt -gt 0 ]
    then
      echo "服务存在，正在停止"
      ps -ef | grep "python36 rest.py" | grep -v grep | awk '{print $2}' | xargs kill
      echo "停止成功"
    else
      echo "服务不存在，无需停止"
    fi
}

cd $ZPD_PYETL_HOME
stop_serv
