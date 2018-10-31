#!/usr/bin/python
# -*- coding: utf-8 -*-
from lxml import etree
import re
import pymysql
import urllib2
import time
import pytz
import datetime


class CommentSpider(object):
    def __init__(self):
        self.current_url = ''
        self.url_id = ''
        self.is_browsed = False

    def start(self, current_url, is_browsed, url_id):
        self.current_url = current_url
        self.url_id = url_id
        self.is_browsed = is_browsed
        return self.read_each_comment()

    def read_each_comment(self):
        # get the imformation of poster
        # driver = webdriver.Chrome()
        # driver.get(self.current_url)
        # html = self.encode_html(driver.page_source)

        # httpproxy_handler = urllib2.ProxyHandler({"https": "118.178.227.171:80"})
        file_path = './ip.txt'
        file_ob = open(file_path, 'r')
        res_ip = file_ob.readlines()
        file_ob.close()
        file_write_ob = open(file_path, 'w')
        set_of_failed_ip = []
        the_index_of_useful_ip = 0
        proxies = {}
        httpproxy_handler = urllib2.ProxyHandler({})
        headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'}
        opener = urllib2.build_opener(httpproxy_handler)
        # s = requests.session()
        # s.keep_alive = False
        # response = requests.get(self.current_url, proxies=proxies)
        request = urllib2.Request(self.current_url, None, headers=headers)
        for i in range(51):
            try:
                print 'START TO CONNECT'
                response = opener.open(request, timeout=5)
                content = response.read()
                response.close()
            except Exception, e:
                if i >= 50:
                    print str(e)
                    for ip in res_ip:
                        file_write_ob.write(ip)
                    file_write_ob.close()
                    return False
                else:
                    if i > 0:
                        set_of_failed_ip.append(res_ip[i-1])
                    print 'RETRY!!!'
                    proxies['https'] = res_ip[i].strip('\n')
                    opener = urllib2.build_opener(urllib2.ProxyHandler(proxies))
                    time.sleep(0.5)
            else:
                if i > 0:
                    the_index_of_useful_ip = i-1
                print 'Connect successfully!!!'
                time.sleep(0.1)
                break
        # put the proxy ips which cannot create valid connection in the end of the file to save time
        for i in range(the_index_of_useful_ip, 50):
            file_write_ob.write(res_ip[i])
        for i in set_of_failed_ip:
            file_write_ob.write(i)
        file_write_ob.close()

        html = self.encode_html(content)
        tree = etree.HTML(html)

        # connect to db
        db = pymysql.connect("localhost", "root", "123456", "douban_rocketgirl101_group")
        cursor = db.cursor()
        table_name = 'temp_' + self.url_id

        # if is new post, check if the post is today, if not ,return
        if not self.is_browsed:
            tz = pytz.timezone('Asia/Shanghai')  # Attention TIP!!! HOW TO PROCESS TIMEZONE
            to_month = datetime.datetime.now(tz).month
            to_day = datetime.datetime.now(tz).day
            or_time_of_the_post = tree.xpath("//*[@id='content']//div[@class='topic-doc']/h3/"
                                             "span[@class='color-green']/text()")[0]
            data = re.findall(r"\d+", or_time_of_the_post)
            post_month = int(data[1])
            post_day = int(data[2])
            if post_day != to_day or post_month != to_month:
                print 'The post is not today!'
                return False
            print 'Create table %s' % table_name
            create_table = "CREATE TABLE %s LIKE base_data" % table_name
            cursor.execute(create_table)
        else:
            truncate_table = "TRUNCATE TABLE %s" % table_name
            cursor.execute(truncate_table)

        # begin to process data
        or_personname = tree.xpath("//*[@class='topic-content clearfix']//div[@class='user-face']/a/img/@alt")
        or_personpage = tree.xpath("//*[@class='topic-content clearfix']//span[@class='from']/a/@href")
        poster_name = or_personname[0]
        cur_poster = poster_name.encode('utf8')
        if '"' in cur_poster:
            cur_poster = cur_poster.replace('"', '\\"')
        if "'" in cur_poster:
            cur_poster = cur_poster.replace("'", "\\'")
        print 'The poster name is %s' % cur_poster

        temp = or_personpage[0].split('/')
        id_of_poster = temp[len(temp) - 2]
        add_post = "INSERT INTO %s(ID,name,post) VALUES ('%s', '%s', '%d')" % (table_name, id_of_poster, cur_poster, 1)
        try:
            cursor.execute(add_post)
        except Exception, e:
            print "The name has problem!"
            cur_poster = "The_name_has_problem"
            add_post = "INSERT INTO %s(ID,name,post) VALUES ('%s', '%s', '%d')" \
                       % (table_name, id_of_poster, cur_poster, 1)
            cursor.execute(add_post)

        # get info in first page
        ids = []
        names = []
        s = set()  # compute the num of comments of this post from unique id
        or_names = tree.xpath("//*[@class='topic-reply']/li[@class='clearfix comment-item']/div/a/img/@alt")
        or_ids = tree.xpath("//*[@class='topic-reply']//div[@class='user-face']/a/@href")

        for id in or_ids:
            temp = id.split('/')
            real_id = temp[len(temp) - 2]
            ids.append(real_id)
            s.add(real_id)

        for name in or_names:
            cur = name.encode('utf8')
            # There may be special character cause exception
            if '"' in cur:
                cur = cur.replace('"', '\\"')
            if "'" in cur:
                cur = cur.replace("'", "\\'")
            names.append(cur)

        # update the database
        for (id, name) in zip(ids, names):
            # !!!ATTENTION TIPS
            print 'Process the data of %s' % name
            sql = "INSERT INTO %s(ID,name,comment) VALUES ('%s', '%s', '%d') \
                ON DUPLICATE KEY UPDATE name='%s', comment=comment+%d" % (table_name, id, name, 1, name, 1)
            try:
                cursor.execute(sql)
            except Exception, e:
                name = "The_name_has_problem"
                sql = "INSERT INTO %s(ID,name,comment) VALUES ('%s', '%s', '%d') \
                                ON DUPLICATE KEY UPDATE name = '%s', comment=comment+%d" % (table_name, id, name, 1,
                                                                                            name, 1)
                cursor.execute(sql)

        # get data of the following page
        page = tree.xpath("//*[@id='content']//div[@class='paginator']/span[@class='thispage']/@data-total-page")
        if len(page) > 0:
            num_of_page = int(page[0])
            print 'This post has %d pages' % num_of_page
            start = 100
            count = 1
            while count < num_of_page:
                flag = False
                cur_url = self.current_url + "?start=%d" % start
                start += 100
                count = count + 1
                request = urllib2.Request(cur_url, None, headers=headers)
                file_write_ob = open(file_path, 'w')
                set_of_failed_ip = []
                the_index_of_useful_ip = 0
                for i in range(51):
                    try:
                        print 'START TO CONNECT'
                        response = opener.open(request, timeout=5)
                        content = response.read()
                        response.close()
                    except Exception, e:
                        if i >= 50:
                            print str(e)
                            for ip in res_ip:
                                file_write_ob.write(ip)
                            file_write_ob.close()
                            flag = True
                        else:
                            if i > 0:
                                set_of_failed_ip.append(res_ip[i - 1])
                            print 'RETRY!!!'
                            proxies['https'] = res_ip[i].strip('\n')
                            opener = urllib2.build_opener(urllib2.ProxyHandler(proxies))
                            time.sleep(0.5)
                    else:
                        if i > 0:
                            the_index_of_useful_ip = i-1
                        print 'Connect successfully!!!'
                        time.sleep(0.1)
                        break

                if flag:
                    continue

                for i in range(the_index_of_useful_ip, 50):
                    file_write_ob.write(res_ip[i])
                for i in set_of_failed_ip:
                    file_write_ob.write(i)
                file_write_ob.close()

                html = self.encode_html(content)
                tree = etree.HTML(html)
                ids = []
                names = []
                or_names = tree.xpath("//*[@class='topic-reply']/li[@class='clearfix comment-item']/div/a/img/@alt")
                or_ids = tree.xpath("//*[@class='topic-reply']//div[@class='user-face']/a/@href")

                for id in or_ids:
                    temp = id.split('/')
                    real_id = temp[len(temp) - 2]
                    ids.append(real_id)
                    s.add(real_id)

                for name in or_names:
                    cur = name.encode('utf8')
                    # There may have special character which will cause exception
                    if '"' in cur:
                        cur = cur.replace('"', '\\"')
                    elif "'" in cur:
                        cur = cur.replace("'", "\\'")
                    names.append(cur)

                for (id, name) in zip(ids, names):
                    # !!!ATTENTION TIPS
                    print 'Process the data of %s' % name
                    sql = "INSERT INTO %s(ID,name,comment) VALUES ('%s', '%s', '%d') \
                        ON DUPLICATE KEY UPDATE name = '%s', comment=comment+%d" % (
                        table_name, id, name, 1, name, 1)
                    try:
                        cursor.execute(sql)
                    except Exception, e:
                        name = "The_name_has_problem"
                        sql = "INSERT INTO %s(ID,name,comment) VALUES ('%s', '%s', '%d') \
                                        ON DUPLICATE KEY UPDATE name = '%s', comment=comment+%d" % (
                            table_name, id, name, 1, name, 1)
                        cursor.execute(sql)
        else:
            print 'This post has 1 page'

        # add the num of comment to the poster
        add_num_of_comment = "INSERT INTO %s(ID,comment_got) VALUE ('%s', '%d') \
                                    ON DUPLICATE KEY UPDATE comment_got=comment_got+%d" % (
                                    table_name, id_of_poster, len(s), len(s))
        cursor.execute(add_num_of_comment)
        db.commit()
        # driver.close()
        cursor.close()
        db.close()
        return True

    def encode_html(self, html):
        return html.decode('utf8')
