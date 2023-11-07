import xml.sax


class Ele:
    """
    Book 类, 用于保存 xml 中 <book/> 元素节点的数据
    """
    creationTime = ""
    acceptAckCode = ""
    interactionId = ""

    def __str__(self):
        return "Ele: creationTime=%s, acceptAckCode=%s, interactionId=%s" % (
            self.creationTime, self.acceptAckCode, self.interactionId)


class BooksContentHandler(xml.sax.handler.ContentHandler):

    def __str__(self):
        return self.__ele.__str__()  # 临时解析到的 Book

    """
    用于解析 Books XML 的 Handler
    """

    __current_element_name = None  # 当前解析到的元素节点名称

    def startDocument(self):
        """ 文档开始 """
        # 文档开始, 重置 books
        self.__ele = Ele()

    def endDocument(self):
        """ 文档结束 """
        pass

    def startElement(self, name, attrs):
        """ 元素开始 """
        # 元素开始, 保存当前元素节点名称
        self.__current_element_name = name
        if name == "creationTime":
            self.__ele.creationTime = attrs.getValue("value")
        if name == "item":
            self.__ele.interactionId = attrs.getValue("extension")
        if name == "acceptAckCode":
            self.__ele.acceptAckCode = attrs.getValue("code")
    # <book/> 元素开始, 新建 Book 对象
    # 收集 <book/> 元素的属性值
    # self.__ele.creationTime = attrs.getValue("value")


if __name__ == "__main__":
    # 创建 SAX 解析器
    parser = xml.sax.make_parser()

    # 创建用于解析 Books XML 的 Handler
    handler = BooksContentHandler()

    # 给解析器设置 Handler
    parser.setContentHandler(handler)
    str = "<POOR_IN200901UV ITSVersion=\"XML_1.0\" xmlns=\"urn:hl7-org:v3\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"urn:hl7-org:v3 ../multicacheschemas/POOR_IN200901UV.xsd\">\n" \
          "	<id root=\"2.16.156.10011.2.5.1.1\" extension=\"00A216651299404CA02A51BF6A2D0183\"/>\n" \
          "	<creationTime value=\"20220824000002\"/>\n" \
          "	<interactionId root=\"2.16.156.10011.2.5.1.2\" extension=\"POOR_IN200901UV\"/>\n" \
          "	<processingCode code=\"P\"/>\n" \
          "	<processingModeCode/>\n" \
          "	<acceptAckCode code=\"AL\"/>\n" \
          "	<receiver typeCode=\"RCV\">\n" \
          "		<telecom/>\n" \
          "		<device classCode=\"DEV\" determinerCode=\"INSTANCE\">\n" \
          "			<id>\n" \
          "				<item root=\"2.16.156.10011.2.5.1.3\" extension=\"Ny5VJ9kFJI+xbiITlRIqCzCLnjhcQ8rWL/Okq9c4dOg=\"/>\n" \
          "			</id>\n" \
          "		</device>\n" \
          "	</receiver>\n" \
          "	<sender typeCode=\"SND\">\n" \
          "		<telecom/>\n" \
          "		<device classCode=\"DEV\" determinerCode=\"INSTANCE\">\n" \
          "			<id>\n" \
          "				<item root=\"2.16.156.10011.2.5.1.3\" extension=\"7XMFnK+UyKuly9FX/+UueII1VJdiR3eAJAMajmQClb4=\"/>\n" \
          "			</id>\n" \
          "		</device>\n" \
          "	</sender>\n" \
          "</POOR_IN200901UV>"
    # 解析 XML 文档
    xml.sax.parseString(str, handler)

    # xml.sax.parse("books.xml", handler)       # 可以使用一个方法直接解析
    # xml.sax.parseString(xmlstring, handler)   # 可以直接解析一段 xml 文本

    # 从 Handler 中获取解析结果输出
    print(handler)
