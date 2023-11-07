#!/usr/bin/env bash
set -o errexit
# 安装包所在主目录，安装包名为：python36.zip、zpd-pyetl-版本号.zip
base_dir=/zpd
zpd_pyetl=zpd-pyetl-4.0.8

cd $base_dir
unzip python36.zip
unzip ${zpd_pyetl}.zip
cd $base_dir/python36

# 解压各个压缩包
unzip instantclient-basic-linux.x64-18.5.0.0.0dbru.zip
tar xzvf Python-3.6.10.tgz

# 配置Oracle客户端环境
cd $base_dir/python36/instantclient_18_5
mkdir lib
ln -s $base_dir/python36/instantclient_18_5/*.so lib/
echo "ZPD_PYETL_HOME=${base_dir}/${zpd_pyetl}" >> ~/.bash_profile
echo "PATH=${base_dir}/python36/instantclient_18_5:\$PATH:\$HOME/bin" >> ~/.bash_profile
echo "export LD_LIBRARY_PATH=${base_dir}/python36/instantclient_18_5:\$LD_LIBRARY_PATH" >> ~/.bash_profile
echo "export PATH" >> ~/.bash_profile
source ~/.bash_profile

# 编译 Python 3.6.10
cd $base_dir/python36/Python-3.6.10
mkdir /usr/local/python3
./configure --prefix=/usr/local/python3
make
make install

ln -s /usr/local/python3/bin/python3.6 /usr/local/bin/python36
ln -s /usr/local/python3/bin/python3.6 /usr/bin/python36
ln -s /usr/local/python3/bin/pip3 /usr/local/bin/pip36
ln -s /usr/local/python3/bin/pip3 /usr/bin/pip36
# 安装各类依赖包
cd $base_dir/python36
python36 install.py



# 注释掉pandas，__init__.py文件的第120行代码
sed -i "120s/^/#/g" /usr/local/python3/lib/python3.6/site-packages/pandas/compat/__init__.py
