#!/bin/bash
source ~/.bash_profile

#函数定义
function restart_serv(){
    cnt=`ps -ef | grep "python3 rest.py" | grep -v grep | wc -l`
    if [ $cnt -gt 0 ]
    then
      echo "ETL服务异常，正在重启"
      ps -ef | grep "python3 rest.py" | grep -v grep | awk '{print $2}' | xargs kill
      nohup python3 rest.py >> $ZPD_PYETL_HOME/log/py_rest.log 2>&1 &
      echo "重启成功"
    else
      nohup python3 rest.py >> $ZPD_PYETL_HOME/log/py_rest.log 2>&1 &
      echo "服务不存在，重启成功"
    fi
}

cd $ZPD_PYETL_HOME

# 判断该脚本是否重复运行
restcnt=`ps -ef | grep "check_rest_status" | grep -v grep | wc -l`
if [ $restcnt -gt 0 ]
then
  echo "检查脚本重复运行，直接重启"
  ps -ef | grep "check_rest_status" | grep -v grep | awk '{print $2}' | xargs kill
  restart_serv
else
# 超时设置为5s
  result=`curl --connect-timeout 10 -m 10 "http://127.0.0.1:8383/check_rest_status/"`

  if [ -n "$result" ]
  then
    if [ $result -eq "0" ]
    then
      echo "ETL服务正常"
    else
      restart_serv
      echo "链接超时，ETL服务重启完毕"
    fi
  else
    cnt=`ps -ef | grep "python3 rest.py" | grep -v grep | wc -l`
    restart_serv
  fi
fi

cd $ZPD_PYETL_HOME/log

yestoday_str=$(date -d "yesterday" +%Y%m%d)

if [ ! -f "py_rest.log.${yestoday_str}" ];then
  cp py_rest.log py_rest.log.${yestoday_str}
  echo "" > py_rest.log
  echo "日志文件转存成功"
else
  echo "日志文件已存在"
fi

#日志保留7天
find $ZPD_PYETL_HOME/log -name "py_rest.log.*" -type f -mtime +7 -exec rm -rf {} \;
