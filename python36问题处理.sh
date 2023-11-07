1.报错如下：
ImportError: libbz2.so.1.0: cannot open shared object file: No such file or directory

检查是否已经存在：
cd /usr/lib64/
ls -alhtr | grep libbz2
ln -s libbz2.so.1.0.6 libbz2.so.1.0

再次运行即可，或者重新安装即可


[root@localhost zpd-pyetl]# find / -name 'libbz2*' -ls
165140    0 lrwxrwxrwx   1 root     root           23 Aug 16 17:30 /usr/lib64/libbz2.so -> ../../lib64/libbz2.so.1
1186477   72 -rwxr-xr-x   1 root     root        69976 Jun 25  2011 /lib64/libbz2.so.1.0.4
1179737    0 lrwxrwxrwx   1 root     root           15 Oct 24  2018 /lib64/libbz2.so.1 -> libbz2.so.1.0.4

cd /lib64
ln -s /lib64/libbz2.so.1.0.4 libbz2.so.1.0.6
#==============================================================================================================
2.ModuleNotFoundError: No module named '_bz2'

cp python36/_bz2.cpython-36m-x86_64-linux-gnu.so /usr/local/python3/lib/python3.6/lib-dynload/

#==============================================================================================================
3.[GCC 4.8.5 20150623 (Red Hat 4.8.5-28)] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import ssl
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/local/python3/lib/python3.6/ssl.py", line 101, in <module>
    import _ssl             # if we can't import it, let the error propagate
ModuleNotFoundError: No module named '_ssl'


# 修改两个文件
cd $base_dir/python36/Python-3.6.10
vim Modules/Setup.dist
vim Modules/Setup

#去掉注释
# Socket module helper for socket(2)
_socket socketmodule.c

# Socket module helper for SSL support; you must comment out the other
# socket line above, and possibly edit the SSL variable:
SSL=/usr/local/ssl
_ssl _ssl.c \
	-DUSE_SSL -I$(SSL)/include -I$(SSL)/include/openssl \
	-L$(SSL)/lib -lssl -lcrypto


./configure --prefix=/usr/local/python3
make
make install

python3
import ssl
# 成功

#==============================================================================================================
4.UserWarning: Could not import the lzma module. Your installed Python is incomplete
sed -i "120s/^/#/g" /usr/local/python3/lib/python3.6/site-packages/pandas/compat/__init__.py

#==============================================================================================================
