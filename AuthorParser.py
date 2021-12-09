# -*- coding:UTF-8 -*-
import hashlib
import re
import threading
import urllib
import urllib.parse
import urllib.request

from bs4 import BeautifulSoup
from pinyin import pinyin
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

from DatabaseDriver import DatabaseDriver


class AuthorParser(threading.Thread):
    def __init__(self, bs, databaseDriver, lock):
        super().__init__()
        self.bs = bs
        self.databaseDriver = databaseDriver
        self.lock = lock
        self.expertList = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/86.0.4240.183 Safari/537.36"}

    def run(self):
        try:
            # print("bs")
            # print(self.bs)
            for searchResult_textDiv in self.bs.select("div[class='searchResult_text']"):
                # print(searchResult_textDiv)
                self.saveAuthor(searchResult_textDiv)
            self.databaseDriver.insertExpert(self.expertList)
            # print(self.expertList)
        finally:
            self.lock.release()

    def getAuthorName(self, searchResult_textDiv):
        try:
            name = searchResult_textDiv.select("a[class='personName']", limit=1)[0].next_element
        except:
            # try:
            #     name = main_info.h3.span.string
            # except:
            name = ""
        # print(searchResult_textDiv.select("a[class='personName']", limit=1))
        return name.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getAffiliate(self, searchResult_textDiv):
        try:
            affiliate = searchResult_textDiv.select("p[class='personInstitution color_666']", limit=1)[0].string
        except:
            # try:
            #     name = main_info.h3.span.string
            # except:
            affiliate = ""
        return affiliate.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getDomain(self, searchResult_textDiv):
        try:
            domain = searchResult_textDiv.select("span[class='aFiled']", limit=1)[0].string
        except:
            domain = ""
        return ','.join(domain.lstrip().rstrip().replace('\n', '').replace('\r', '').split(" "))

    def getExpertID(self, name, affiliate):
        return 'EX-' + hashlib.md5(str(name + ',' + affiliate).encode('utf-8')).hexdigest()

    def saveAuthor(self, searchResult_textDiv):
        name = self.getAuthorName(searchResult_textDiv)
        affiliate = self.getAffiliate(searchResult_textDiv)
        item = {
            "name": name,
            "affiliate": affiliate,
            "id": self.getExpertID(name, affiliate),
            "domain": self.getDomain(searchResult_textDiv),
        }
        self.expertList.append(item)
