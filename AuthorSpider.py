# -*- coding:UTF-8 -*-
import hashlib
import json
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
from AuthorParser import AuthorParser


class AuthorSpider:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/86.0.4240.183 Safari/537.36"}
        self.databaseDriver = DatabaseDriver(host="124.70.63.71", port=3306, user="root", passwd="root",
                                             database_name="scholar")
        self.numOfDriver = 10
        self.listOfDriver = {}
        for i in range(self.numOfDriver):
            driverAndLock = {
                "driver": DatabaseDriver(host="124.70.63.71", port=3306, user="root", passwd="root",
                                             database_name="scholar"),
                "lock": threading.Lock()
            }
            self.listOfDriver[str(i)] = driverAndLock
            print(str(i) + "号成功连接数据库！")
        self.i = 0

    def releaseDriver(self):
        for driverAndLock in self.listOfDriver.values():
            driverAndLock["driver"].releaseDatabase()

    def authorSearchResultUrlEncode(self, author_name, pageNumber):
        author_name = quote(author_name, encoding="utf-8")
        path = "https://xueshu.baidu.com/usercenter/data/authorchannel?cmd=search_author&_token=d42af69d15e668e785ec6b93c2078ff994cf472c77364f602a14e2063c642f6d&_ts=1607846428&_sign=a0234cb9bbf84aa6a08e18141fdd38a2&author=" + author_name + "&affiliate=&curPageNum=" + str(
            pageNumber)
        # print(path)
        return path

    def authorInformationPageUrlEncode(self, link):
        path = "https://xueshu.baidu.com " + link
        return path

    def getPageNumber(self, keyword):
        try:
            return eval(self.databaseDriver.getPageNumber(keyword))
        except:
            return 0

    def searchAuthorListByKeyWord(self, keyword):
        if self.databaseDriver.authorKeywordExists(keyword):
            print("\"" + keyword + "\"已被爬取完，跳转至下一字")
            return
        for i in range(1, 168):
            request = urllib.request.Request(url=self.authorSearchResultUrlEncode(keyword, i), headers=self.headers)
            response = urllib.request.urlopen(request)
            # time.sleep(1)
            authorListHtml = json.load(response)["htmldata"]
            # print(authorListHtml)
            # print(type(authorListHtml))
            # with open("author.html", mode='w', encoding="utf-8") as file:
            #     file.write(authorListHtml)
            bs = BeautifulSoup(authorListHtml, "html.parser")
            if len(bs.select("div[class='searchResult_text']")) == 0:
                break
            self.listOfDriver[str(i % self.numOfDriver)]["lock"].acquire()
            self.newThreadParse(bs, str(i % self.numOfDriver))

        # print(bs)
        self.databaseDriver.updateAuthorKeyword(keyword)

    def newThreadParse(self, bs, i):
        thread = AuthorParser(bs, self.listOfDriver[i]["driver"], self.listOfDriver[i]["lock"])
        thread.start()


def GBK2312():
    head = random.randint(0xb0, 0xf7)
    body = random.randint(0xa1, 0xfe)
    val = f'{head:x}{body:x}'
    str = bytes.fromhex(val).decode('gb2312')
    return str


def echo():
    authorSpider = AuthorSpider()
    try:
        while (True):
            keyword = GBK2312()
            print("开始爬取关键字：" + keyword)
            authorSpider.searchAuthorListByKeyWord(keyword)
    except Exception as e:
        authorSpider.releaseDriver()
        print("echoError:")
        traceback.print_exc()
        echo()


if __name__ == '__main__':
    echo()
