import execjs

string = 'var createGuid = function(a) {' \
         '		return (((a + Math.random()) * 65536) | 0).toString(16).substring(1)' \
         '	};' \
         ' var guid = createGuid(1) + createGuid(1) + "-" + createGuid(1) + "-" + ' \
         'createGuid(1) + createGuid(1) + "-" + ' \
         'createGuid(1) + createGuid(1) + createGuid(1);' \
         ' var a = 1'

# 通过compile命令转成一个js对象
docjs = execjs.compile(string)

# 调用function
res = docjs.call('createGuid', 2)
print(res)

# 调用变量
res = docjs.eval('guid')
print(res)
