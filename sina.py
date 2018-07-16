# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib.request
import re
import MySQLdb
import time,datetime
import os
import urllib.error
from newspaper import Article

#获得每条新闻的关键字，摘要，正文
def getSinaContent():
    response=urllib.request.urlopen(link)
    data=response.read()
    data=data.decode('utf-8')
    soup=BeautifulSoup(data,'html.parser')
    #print(soup)
    keyword=re.findall(r'meta content="(.*)" name="keywords"',str(soup))
    #list index out of range
    if len(keyword):
        keyword=keyword[0]
    #print(keyword)
    description=re.findall(r'meta content="(.*)" name="description"',str(soup))
    if len(description):
        description=description[0]
    contents=soup.select('#article_content')
    p_article= Article(link,language='zh')
    p_article.download()
    p_article.parse()
    article=p_article.text
    return (keyword,description,article)

#存储数据
def storeDB(channel,title,link,keyword,description,article,ptime):
    # 插入之前检查数据库中是否存在该条数据
    sqlExit = "select link from list2 where link='%s'" % (link)
    res = cursor.execute(sqlExit)
    if res:
        print("数据已存在",res)
        return 0
    #数据不存在
    else:
        try:
            insert_news = ("insert into list2(channel,title,link,keyword,description,article,ptime)" "values(%s,%s,%s,%s,%s,%s,%s)")
            data_news = (channel,title,link,keyword,description,article,ptime)
            try:
                result=cursor.execute(insert_news, data_news)
                #插入成功后返回的id
                insert_id=db.insert_id()
                db.commit()
                #判断是否插入成功
                if result:
                    print("插入成功",insert_id)
                    return insert_id + 1
            # 发生错误时回滚
            except MySQLdb.Error as e:
                db.rollback()
                print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except MySQLdb.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

#time
def newsTime():
    timeArray = time.localtime(ptime)
    otherStyleTime = time.strftime("%Y/%m/%d %H:%M:%S", timeArray)
    return otherStyleTime

#关闭数据库
def closeMysql():
    cursor.close()
    db.close()

url = "http://roll.news.sina.com.cn/interface/rollnews_ch_out_interface.php?col=89&spec=&type=&ch=01&k=&offset_page=0&offset_num=0&num=60&asc=&page="
page=159
#连接数据库
db = MySQLdb.Connect(host="localhost", user="root", passwd="0531", db="sina",charset="utf8")
cursor = db.cursor()

while page:
    try:
        #爬取页面结果
        response=urllib.request.urlopen(url+str(page),timeout=60)
        data=response.read()
        #解码
        data=data.decode('gbk')
        #判断是否为最后一页
        if data.count('channel')==0:
            page = 0
            break
        #提取json中channel,title,link,time
        reg_str = r'channel.*?title : "(.*?)",id.*?title : "(.*?)",url : "(.*?)",type.*?time : (.*?)}'
        pattern = re.compile(reg_str, re.DOTALL)
        items = re.findall(pattern, data)
        for item in items:
            link=item[2]
            #将提取的时间戳转换为整型
            ptime=int(item[3])
            storeDB(item[0],item[1],item[2],getSinaContent()[0],getSinaContent()[1],getSinaContent()[2],newsTime())
        page+=1
    except:
        print("解析页面失败")
closeMysql()
