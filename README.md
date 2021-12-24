# Spider-for-BaiduScholar

一个用Python写的爬虫。主要是为了支持软件系统分析设计的课程设计：[项目地址](https://github.com/AsukaCanDL/Software-System-Analysis-and-Design)

## 说明

- 从百度学术爬取论文和学者数据，同时写入mysql和elasticsearch数据库。
- 只爬取中文数据。
- 必要的数据库格式：
    ```
    ├── Database
    ├── document             // 论文数据
    ├── expert               // 作者数据
    ├── expert_spider_record // 爬取论文时，已访问过的作者
    ├── author_spider_record // 爬取作者时，已访问过的作者
    ```


## 流程 

- 生成随机的gdk编码，找到这个编码对应的中文字符。
- 爬取名称含有这个字符的作者，检查是否已经加入数据库。
- 将作者加入数据库(爬取作者阶段)或者将作者的论文加入数据库(爬去论文阶段)

## 注意

- 先爬取作者，再根据数据库内已有作者爬取论文。
- 先爬取作者，因为爬取论文的逻辑是这个作者在expert裤内且不再expert_spider_record库内，则爬取他的文章。
  

