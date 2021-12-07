# -*- coding:UTF-8 -*-
import hashlib
import random
import re
import threading
import time
import traceback
import urllib.request
import urllib.parse
from urllib.parse import quote
from bs4 import BeautifulSoup
from pinyin import pinyin
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from DatabaseDriver import DatabaseDriver
from PaperParser import PaperParser


class PaperSpider:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/86.0.4240.183 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,"
                      "application/signed-exchange;v=b3;q=0.9"}
        self.databaseDriver = DatabaseDriver(host="49.232.157.22", port=3306, user="BUAA", passwd="BUAA1821",
                                             database_name="BUAA")
        self.numOfDriver = 10
        self.listOfDriver = {}
        for i in range(self.numOfDriver):
            driverAndLock = {
                "driver": DatabaseDriver(host="49.232.157.22", port=3306, user="BUAA", passwd="BUAA1821",
                                         database_name="BUAA"),
                "lock": threading.Lock()
            }
            self.listOfDriver[str(i)] = driverAndLock
            print(str(i) + "号成功连接数据库！")

    def releaseDriver(self):
        for driverAndLock in self.listOfDriver.values():
            driverAndLock["driver"].releaseDatabase()

    def urlEncode(self, keyword, page_number):
        keyword = quote(keyword, encoding="utf-8")
        path = "https://xueshu.baidu.com/s?wd=" + keyword + "&pn=" + str(
            page_number) + "&tn=SE_baiduxueshu_c1gjeupa&ie=utf-8&usm=1&sc_f_para=sc_tasktype%3D%7BfirstSimpleSearch%7D&sc_hit=1"
        return path

    def getPageNumber(self, keyword):
        # return int(eval(self.databaseDriver.getPageNumber(keyword)) / 10) * 10
        # print(self.databaseDriver.getPageNumber(keyword))
        try:
            return eval(self.databaseDriver.getPageNumber(keyword))
        except:
            return 0

    def searchPaperListByKeyWord(self, keyword):
        if self.databaseDriver.paperKeywordExists(keyword):
            print("\"" + keyword + "\"已被爬取完，跳转至下一字")
            return
        # page_number = self.getPageNumber(keyword)
        page_number = 0
        # print("\"" + keyword + "\"" + "领域已爬取到" + str(page_number) + "条数据，本次继续爬取")
        i = 0
        while True:
            request = urllib.request.Request(url=self.urlEncode(keyword, page_number), headers=self.headers)
            response = urllib.request.urlopen(request)
            html = response.read().decode("utf-8")
            self.listOfDriver[str(i)]["lock"].acquire()
            self.newThreadParse(html, str(i))
            page_number += 10
            i = (i + 1) % self.numOfDriver
            if page_number >= 600:
                break
            time.sleep(1)
            # self.databaseDriver.setPageNumber(keyword, str(page_number))
        self.databaseDriver.updateKeyword(keyword)

    def searchPaperListByExpert(self, keyword):
        try:
            if self.databaseDriver.expertExists(keyword):
                print("\"" + keyword + "\"已被爬取完，跳转至下一字")
                return
            rows = self.databaseDriver.getExpert(keyword)
            if len(rows) == 0:
                print("\"" + keyword + "\"expert库内无此关键字作者")
                return
            i = 0
            for row in rows:
                expertName = row[0]
                print("爬取" + expertName + "作者的文章")
                page_number = 0
                while True:
                    request = urllib.request.Request(url=self.urlEncode(expertName, page_number), headers=self.headers)
                    response = urllib.request.urlopen(request)
                    time.sleep(5)
                    html = response.read().decode("utf-8")
                    print(html)
                    self.listOfDriver[str(i)]["lock"].acquire()
                    self.newThreadParse(html, str(i))
                    page_number += 10
                    i = (i + 1) % self.numOfDriver
                    if page_number >= 20:
                        break

                self.databaseDriver.updateExpertName(expertName)
        except:
            traceback.print_exc()

    def newThreadParse(self, html, i):
        thread = PaperParser(html, self.listOfDriver[i]["driver"], self.listOfDriver[i]["lock"])
        thread.start()


def GBK2312():
    head = random.randint(0xb0, 0xf7)
    body = random.randint(0xa1, 0xfe)
    val = f'{head:x}{body:x}'
    str = bytes.fromhex(val).decode('gb2312')
    return str


def echo():
    paperSpider = PaperSpider()
    try:
        while True:
            keyword = GBK2312()
            # keyword = "钟南山"
            print("开始爬取关键字：" + keyword)
            # paperSpider.searchPaperListByKeyWord(keyword)
            paperSpider.searchPaperListByExpert(keyword)
    except Exception as e:
        paperSpider.releaseDriver()
        print("echoError:")
        traceback.print_exc()
        echo()


if __name__ == '__main__':
    echo()
