from PIL import Image
import pytesseract

# 读取图片
image = Image.open('身份证图片.jpg')

# 提取文字
text = pytesseract.image_to_string(image, lang='chi_sim')

# 在提取的文字中查找身份证号码
id_number = re.search(r'\d{17}[\dXx]$', text)

if id_number:
    print("提取到的身份证号码是：" + id_number.group())
else:
    print("未能提取到身份证号码。")