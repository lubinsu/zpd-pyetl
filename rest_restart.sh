#!/bin/bash
source ~/.bash_profile

#函数定义
function restart_serv(){
    cnt=`ps -ef | grep "python36 rest.py" | grep -v grep | wc -l`
    if [ $cnt -gt 0 ]
    then
      echo "服务存在，正在重启"
      ps -ef | grep "python36 rest.py" | grep -v grep | awk '{print $2}' | xargs kill
      nohup python36 rest.py >> $ZPD_PYETL_HOME/log/py_rest.log 2>&1 &
      echo "重启成功"
    else
      nohup python36 rest.py >> $ZPD_PYETL_HOME/log/py_rest.log 2>&1 &
      echo "服务不存在，重启成功"
    fi
}

cd $ZPD_PYETL_HOME
restart_serv

