
```
支持数据库：Mysql、Oracle、Postgre、sqlserver、iris

支持的操作：异构数据库之间表对表抽取、变量传递、存储调用、SQL脚本调用、动态语句拼接、单条数据轮询同步（转换）、HTTP调用（POST、GET）、WebService调用、JavaScript代码调用
xml解析、json解析、xml、json解析后入库、调用shell脚本等，并且可在原基础上很方便的进行扩展

新功能：支持BLOB字段数据抽取、支持多线程任务
执行通过调用rest服务来发起etl任务
```

# zpd-pyetl

以下说明已经可以通过一键安装脚本实现部署：zpd-pyetl.sh

 欢迎沟通交流：
> * 微信号：snoopy-lubin
> * 公众号：玩大数据的snoopy
> * CSDN：https://blog.csdn.net/lubinsu?type=blog

### Python 版 ETL中间件

#### QUICK START

##### 运行环境：
> * Python 3.6.10
> * Linux/Windows

##### 拷贝源码
将最新的zpd-pyetl项目整个文件夹拷贝到服务器目录下，如：/soft/etl/etler/zpd-pyetl

##### 配置ZPD_PYETL_HOME
```
vim ~/.bash_profile
ZPD_PYETL_HOME=/soft/etl/etler/zpd-pyetl
PATH=$ZPD_PYETL_HOME::$PATH:$HOME/bin
export PATH
```

##### 配置文件模板参考：
> * ${ZPD_PYETL_HOME}/resources/config-template.xml

###### 数据库连接密码加密
配置中的数据库密码需要进行加密，加密方式如下
```shell script
python3 $ZPD_PYETL_HOME/encryption/base64_encode.py
选择操作编号 1:编码、2:解码、3:退出 :
1
编码要加密的字符串:
123456
编码后字符串:
TVRJek5EVTI=
```
加密后将密码：TVRJek5EVTI=  配置到配置文件中

##### 运行示例：
```shell script
#!/bin/bash
source ~/.bash_profile
cd $ZPD_PYETL_HOME
# job以逗号隔开
python3 main.py zhuyuan_fuzhujiancha.xml jobname1,jobname2,jobname3
# 数据库配置方式
python3 main.py db jobname1,jobname2,jobname3
```

##### 日志所在目录
cd ${ZPD_PYETL_HOME}/log/

##### 注意事项
> * 配置过程中from源字段必须严格和目标表的字段一致，from的字段必须在目标表中存在
------
#### 附
##### Python 3.6.10安装部署
###### 1、安装python3
> * 解压编译
```shell script
tar xzvf Python-3.6.10.tgz
cd Python-3.6.10
mkdir /usr/local/python3
./configure --prefix=/usr/local/python3
make
make install
```

创建软连接：
```shell script
ln -s /usr/local/python3/bin/python3.6 /usr/local/bin/python3
ln -s /usr/local/python3/bin/python3.6 /usr/bin/python3
ln -s /usr/local/python3/bin/pip3 /usr/local/bin/pip3
```
###### 2、目录下，各个whl结尾的文件均安装一遍：
```shell script
pip3 install  setuptools-57.0.0-py3-none-any.whl
pip3 install  setuptools_scm-6.0.1-py3-none-any.whl
pip3 install  PyMySQL-1.0.2-py3-none-any.whl
pip3 install  Cython-0.28.5-cp36-cp36m-manylinux1_x86_64.whl
pip3 install  cx_Oracle-8.2.1-cp36-cp36m-manylinux1_x86_64.whl
pip3 install  pymssql-2.2.1-cp36-cp36m-manylinux1_x86_64.whl
```
###### 3、petl安装
```shell script
tar -xzvf petl-1.7.4.tar.gz
cd petl-1.7.4/
python3 setup.py install
```
PS:如果报错：ModuleNotFoundError: No module named '_ctypes'
安装：yum install libffi-devel 再从头重新编译安装 Python

###### 4、zpd-pyetl 部署
将打包的项目直接拷贝到服务器目录解压即可使用

###### 5、搭建virtualenv
```
pip3 install distlib-0.3.2-py2.py3-none-any.whl 
pip3 install six-1.16.0-py2.py3-none-any.whl
pip3 install appdirs-1.4.4-py2.py3-none-any.whl
pip3 install filelock-3.0.12-py3-none-any.whl
pip3 install virtualenv-20.4.7-py2.py3-none-any.whl

ln -s /usr/local/python3/bin/virtualenv /usr/local/bin/virtualenv3

cd /zpd/zpd-pyetl
virtualenv pyetl_env
source pyetl_env/bin/activate
```
