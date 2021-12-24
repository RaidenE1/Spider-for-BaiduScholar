# -*- coding:UTF-8 -*-
import pymysql
from elasticsearch import Elasticsearch


class DatabaseDriver:

    def __init__(self, host, port, user, passwd, database_name):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database_name = database_name
        self.db = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=database_name, charset='utf8')  # 打开数据库连接

        self.cursor = self.db.cursor()  # 使用 cursor() 方法创建一个游标对象 cursor

    def releaseDatabase(self):
        self.db.close()

    def insertPapers(self, paperList):
        sql = "INSERT INTO document(title, experts, dtype, documentid, time_, doi, isbn, application_number, cited_quantity, summary, keywords, link, origin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for item in paperList:
            print(item)
            if not item['time']:
                item['time'] = '1900-01-01'
            else:
                time_list = item['time'].split('-')
                if len(time_list) < 3:
                    item['time'] = time_list[0] + '-01-01'
                else:
                    if time_list[1] == '00':
                        time_list[1] = '01'
                    if time_list[2] == '00':
                        time_list[2] = '01'
                    item['time'] = '-'.join(time_list)
            if not item["citedQuantity"]:
                item["citedQuantity"] = 0
            try:
                self.cursor.execute(sql, (item["title"], ',' + ','.join(item["authors"]) + ',', item["category"], item["id"],
                                          item["time"],
                                          item["DOI"], item["ISBN"], item['patentNumber'],
                                          item["citedQuantity"],
                                          item["abstract"], ','.join(item["keywords"]), item["link"], item["source"]))
                self.db.commit()
                es = Elasticsearch(hosts="124.70.63.71", port=9200)
                data = {"documentid": item["id"], "title": item["title"], "dtype": item["category"], "experts": ',' + ','.join(item["authors"]) + ','\
                    , "summary" : item["abstract"], "cited_quantity" : item["citedQuantity"],\
                        "link" : item["link"], "origin" : item["source"], "time" : item["time"], "is_favor" : False, "views" : 0}
                if len(item["keywords"]) == 1 and '；' in item["keywords"][0]:
                    item["keywords"] = item["keywords"][0].split("；")
                data["keywords"] = ','.join(item["keywords"])
                res = es.index(index="document3", doc_type = "_doc", body = data)
                print("Insert doc successfully")
                for _keyword in item["keywords"]:
                    body = {
                        "query":{
                            "term":{
                                "keyword" : _keyword
                            }
                        }
                    }
                    res = es.search(index="keyword", doc_type = "_doc", body = body)
                    if res["hits"]["total"]["value"] == 0:
                        data = {"keyword" : _keyword, "view" : 0, "citedNum" : 1}
                        res = es.index(index="keyword", doc_type = "_doc", body = data)
                    else:
                        assert res["hits"]["total"]["value"] == 1
                        data = {"keyword" : _keyword, "view" : res["hits"]["hits"][0]["_source"]["view"], "citedNum" : res["hits"]["hits"][0]["_source"]["citedNum"] + 1}
                        res = es.index(index="keyword", doc_type = "_doc", id = res["hits"]["hits"][0]["_id"], body = data)
                    print(res)
                print("Insert successfully!")
            except Exception as e:
                self.db.rollback()
                print("Fail:")
                print(e)


    def insertExpert(self, expertList):
        sql = "INSERT INTO expert(expertid, name, org, domain) VALUES (%s, %s, %s, %s)"
        for item in expertList:
            print(item)
            try:
                self.cursor.execute(sql, (item["id"], item["name"], item["affiliate"], item["domain"]))
                self.db.commit()
                es = Elasticsearch(hosts="124.70.63.71", port=9200)
                data = {"expertid" : item["id"], "name" : item["name"], "org" : item["affiliate"], "domain" : item["domain"], "cooperationNum" : 0}
                res = es.index(index="expert", doc_type = "_doc", body = data)
                print(res)
                print("Insert successfully!")
            except Exception as e:
                self.db.rollback()
                print("Fail:", end="")
                print(e)

    def getExpert(self, keyword):
        sql = "SELECT name FROM expert WHERE name like %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, "%" + keyword + "%")
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("关键词数据库记录查询失败:", end="")
            print(e)

    def getPageNumber(self, keyword):
        sql = "SELECT quantity FROM paper_spider_record WHERE name= %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, keyword)
            results = self.cursor.fetchall()
            if len(results) == 0:
                return 0
            for row in results:
                return row[0]
        except Exception as e:
            print("Fail:", end="")
            print(e)

    def paperKeywordExists(self, keyword):
        sql = "SELECT name FROM paper_spider_record WHERE name= %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, keyword)
            results = self.cursor.fetchall()
            if len(results) == 0:
                return False
            else:
                return True
        except Exception as e:
            print("关键词数据库记录查询失败:", end="")
            print(e)

    def expertExists(self, expertName):
        sql = "SELECT name FROM expert_spider_record WHERE name= %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, expertName)
            results = self.cursor.fetchall()
            if len(results) == 0:
                return False
            else:
                return True
        except Exception as e:
            print("关键词数据库记录查询失败:", end="")
            print(e)

    def authorKeywordExists(self, keyword):
        sql = "SELECT name FROM author_spider_record WHERE name= %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, keyword)
            results = self.cursor.fetchall()
            if len(results) == 0:
                return False
            else:
                return True
        except Exception as e:
            print("关键词数据库记录查询失败:", end="")
            print(e)

    def updateKeyword(self, keyword):
        try:
            sql = "INSERT INTO paper_spider_record(name) VALUES (%s)"
            self.cursor.execute(sql, keyword)
            self.db.commit()
            print("Update record successfully!")
        except Exception as e:
            self.db.rollback()
            print("数据库更新关键词失败:", end="")
            print(e)

    def updateExpertName(self, expertName):
        try:
            sql = "INSERT INTO expert_spider_record(name) VALUES (%s)"
            self.cursor.execute(sql, expertName)
            self.db.commit()
            print("Update expertName successfully!")
        except Exception as e:
            self.db.rollback()
            print("expert数据库更新关键词失败:", end="")
            print(e)

    def updateAuthorKeyword(self, keyword):
        try:
            sql = "INSERT INTO author_spider_record(name) VALUES (%s)"
            self.cursor.execute(sql, keyword)
            self.db.commit()
            print("Update record successfully!")
        except Exception as e:
            self.db.rollback()
            print("数据库更新关键词失败:", end="")
            print(e)

    def setPageNumber(self, keyword, number):
        sql = "SELECT quantity FROM paper_spider_record WHERE name= %s"
        # 使用 execute()  方法执行 SQL 查询
        try:
            self.cursor.execute(sql, keyword)
            results = self.cursor.fetchall()
            if len(results) == 0:
                sql = "INSERT INTO paper_spider_record(name, quantity) VALUES (%s, %s)"
                self.cursor.execute(sql, (keyword, number))
            else:
                sql = "UPDATE paper_spider_record SET quantity = %s WHERE name = %s"
                self.cursor.execute(sql, (number, keyword))
            self.db.commit()
            print("Update record successfully!")
        except Exception as e:
            self.db.rollback()
            print("Fail:", end="")
            print(e)


# if __name__ == '__main__':
#     databaseDriver = DatabaseDriver(host="49.232.157.22", port=3306, user="BUAA", passwd="BUAA1821",
#                                     database_name="BUAA")
#     print(len(databaseDriver.getExpert("5")))
