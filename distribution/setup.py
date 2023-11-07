from setuptools import setup, find_packages

# python3 setup.py bdist_wheel
setup(name='zpd-pyetl',
      version='1.0.0',
      author=['lubinsu'],
      description='This is the basic toolkit package of the data platform',
      # packages=find_packages(exclude=('cftool',)),
      install_requires=['pymysql', 'petl', 'pymssql', 'cx_Oracle'])
