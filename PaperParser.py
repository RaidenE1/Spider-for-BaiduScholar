# -*- coding:UTF-8 -*-
import hashlib
import re
import threading
import time
import urllib
import urllib.parse
import urllib.request

from bs4 import BeautifulSoup
from pinyin import pinyin

from DatabaseDriver import DatabaseDriver


class PaperParser(threading.Thread):
    def __init__(self, html, databaseDriver, lock):
        super().__init__()
        self.html = html
        self.databaseDriver = databaseDriver
        self.lock = lock
        self.paperList = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/86.0.4240.183 Safari/537.36"}

    def run(self):
        try:
            bs = BeautifulSoup(self.html, "html.parser")
            paper_link = bs.select("h3[class='t c_font']")
            # print("hhh")
            for li in paper_link:
                self.savePaperByUrl(li.a.attrs['href'])
                time.sleep(5)
            self.databaseDriver.insertPapers(self.paperList)
        finally:
            self.lock.release()

    def getTitle(self, main_info):
        try:
            title = main_info.h3.a.next_element
        except:
            try:
                title = main_info.h3.span.next_element
            except:
                title = ""
        return title.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getAuthors(self, main_info):
        authorList = []
        try:
            for author in main_info.select("p[class='author_text']", limit=1)[0].select("span"):
                authorList.append(author.a.string.lstrip().rstrip().replace('\n', '').replace('\r', ''))
        except:
            try:
                for author in main_info.select("p[class='author_text kw_main_s']", limit=1)[0].select("span"):
                    authorList.append(author.a.string.lstrip().rstrip().replace('\n', '').replace('\r', ''))
            except:
                try:
                    for author in main_info.select(".author_wr", limit=1)[0].select(".kw_main_l")[0].select("span"):
                        authorList.append(author.a.string.lstrip().rstrip().replace('\n', '').replace('\r', ''))
                except:
                    authorList = []
        return authorList

    def getAbstract(self, main_info):
        try:
            abstract = main_info.select(".abstract", limit=1)[0].string
        except:
            abstract = ""
        return abstract.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getKeyWord(self, main_info):
        keywordList = []
        try:
            for keyword in main_info.select(".kw_wr", limit=1)[0].select(".kw_main", limit=1)[0].select("span"):
                keywordList.append(keyword.a.string.lstrip().rstrip().replace('\n', '').replace('\r', ''))
        except:
            try:
                for keyword in main_info.select(".kw_wr", limit=1)[0].select(".kw_main_s", limit=1)[0].select("span"):
                    keywordList.append(keyword.a.string.lstrip().rstrip().replace('\n', '').replace('\r', ''))
            except:
                keywordList = []
        return keywordList

    def getLink(self, main_info):
        try:
            link = main_info.h3.a["href"]
        except:
            link = ""
        return link

    def getTime(self, main_info):
        try:
            time = main_info.select(".year_wr", limit=1)[0].select(".kw_main", limit=1)[
                0].string.lstrip().rstrip().replace('\n', '').replace('\r', '').replace('年', '-').replace('月',
                                                                                                          '-').replace(
                '日', '').replace('.', '-').replace('/', '-')
        except:
            try:
                time = main_info.select(".year_wr", limit=1)[0].select(".kw_main_s", limit=1)[
                    0].string.lstrip().rstrip().replace('\n', '').replace('\r', '').replace('年', '-').replace('月',
                                                                                                              '-').replace(
                    '日', '').replace('.', '-').replace('/', '-')
            except:
                try:
                    for t in main_info.select("div[class='common_wr']"):
                        if t.p.string.lstrip().rstrip().replace('\n', '').replace('\r', '') == "会议时间：":
                            if len(t.select(".kw_main_s", limit=1)) > 0:
                                time = t.select(".kw_main_s", limit=1)[0].string
                            else:
                                time = t.select(".kw_main", limit=1)[0].string
                            break
                        elif t.p.string.lstrip().rstrip().replace('\n', '').replace('\r', '') == "申请日期：":
                            time = t.select(".kw_main_l", limit=1)[0].string
                            break
                    else:
                        time = ""
                except:
                    try:
                        time = main_info.select(".year_wr", limit=1)[0].select(".kw_main_s", limit=1)[0].string
                    except:
                        time = ""
        time = time.lstrip().rstrip().replace('\n', '').replace('\r', '').replace('\r', '').replace('年', '-').replace(
            '月', '-').replace('日', '').replace('.', '-').replace('/', '-')
        time = re.sub("[^0-9-]", "", time)
        if len(time) == 4:
            time = time + "-00-00"
        elif len(time) == 7 or len(time) == 6:
            time = time + "-00"
        return time

    def getDOI(self, main_info):
        try:
            DOI = main_info.select(".doi_wr", limit=1)[0].select(".kw_main", limit=1)[0].string
        except:
            DOI = ""
        return DOI.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getISBN(self, main_info):
        try:
            ISBN = main_info.select(".doi_wr", limit=1)[0].select(".kw_main", limit=1)[0].string
        except:
            ISBN = ""
        return ISBN.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getPatentNumber(self, main_info):
        try:
            for t in main_info.select("div[class='common_wr']"):
                if t.p.string.lstrip().rstrip().replace('\n', '').replace('\r', '') == "申请(专利)号：":
                    patentNumber = t.select(".kw_main_l", limit=1)[0].string
                    break
            else:
                patentNumber = ""
        except:
            patentNumber = ""
        return patentNumber.lstrip().rstrip().replace('\n', '').replace('\r', '').replace('\r', '')

    def getCitedQuantity(self, main_info):
        try:
            cited_quantity = main_info.select(".ref_wr", limit=1)[0].select(".ref-wr-num", limit=1)[0].a.string
        except:
            cited_quantity = ""
        return cited_quantity.lstrip().rstrip().replace('\n', '').replace('\r', '')

    def getCategory(self, dtl_journal):
        try:
            if dtl_journal.h3.string == "来源期刊":
                return "期刊"
            elif dtl_journal.h3.string == "来源出版社" or dtl_journal.h3.string == "来源图书":
                return "图书"
            elif dtl_journal.h3.string == "来源会议":
                return "会议"
            elif dtl_journal.h3.string == "来源学校":
                return "学位"
            else:
                return ""
        except:
            return ""

    def getSource(self, dtl_journal):
        try:
            return dtl_journal.select(".journal_title")[0].string
        except:
            return ""

    def getID(self, title, authors, category):
        return pinyin.get_initial(category, '').upper() + '-' + hashlib.md5(
            str(title + ',' + ','.join(authors)).encode('utf-8')).hexdigest()

    def savePaperByUrl(self, url):
        request = urllib.request.Request(url=url, headers=self.headers)
        response = urllib.request.urlopen(request)
        # time.sleep(1)
        html = response.read().decode("utf-8")
        bs = BeautifulSoup(html, "html.parser")
        main_info = bs.select("div[class='main-info']", limit=1)
        dtl_journal = bs.select("div[class='dtl_journal']", limit=1)
        title = self.getTitle(main_info[0])
        authors = self.getAuthors(main_info[0])
        try:
            category = self.getCategory(dtl_journal[0])
        except:
            category = "专利"
        try:
            source = self.getSource(dtl_journal[0])
        except:
            source = ""
        item = {
            "title": title,
            "authors": authors,
            "category": category,
            "id": self.getID(title, authors, category),
            "time": self.getTime(main_info[0]),
            "DOI": self.getDOI(main_info[0]),
            "ISBN": self.getISBN(main_info[0]),
            "patentNumber": self.getPatentNumber(main_info[0]),
            "citedQuantity": self.getCitedQuantity(main_info[0]),
            "abstract": self.getAbstract(main_info[0]),
            "keywords": self.getKeyWord(main_info[0]),
            "link": self.getLink(main_info[0]),
            "source": source
        }
        self.paperList.append(item)
