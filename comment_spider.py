#!/usr/bin/python
# -*- coding: utf-8 -*-
from lxml import etree
import re
import pymysql
import pytz
import datetime
import get_data_from_url


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
        get_data = get_data_from_url.GetDataFromUrl(self.current_url)
        content = get_data.get_data()
        # check if open the url successfully
        if content == "No":
            return False
        html = content.decode('utf8')
        tree = etree.HTML(html)
        original_poster_name = tree.xpath("//*[@class='topic-content clearfix']//div[@class='user-face']/a/img/@alt")
        original_poster_id = tree.xpath("//*[@class='topic-content clearfix']//span[@class='from']/a/@href")
        # check if get the true content
        try:
            poster_name = original_poster_name[0]
        except Exception, e:
            return False
        # connect to db
        db = pymysql.connect("localhost", "root", "123456", "douban_rocketgirl101_group")
        cursor = db.cursor()
        table_name = 'temp_' + self.url_id

        # if is new post, check if the post is today, if not ,return
        if not self.is_browsed:
            tz = pytz.timezone('Asia/Shanghai')  # Attention TIP!!! HOW TO PROCESS TIMEZONE
            to_month = datetime.datetime.now(tz).month
            to_day = datetime.datetime.now(tz).day
            time_of_the_post = tree.xpath("//*[@id='content']//div[@class='topic-doc']/h3/"
                                          "span[@class='color-green']/text()")[0]
            data = re.findall(r"\d+", time_of_the_post)
            post_month = int(data[1])
            post_day = int(data[2])
            if post_day != to_day or post_month != to_month:
                print 'The post is not today!'
                return False
            print 'Create table %s' % table_name
            create_table = "CREATE TABLE %s LIKE base_data" % table_name
            # if program has been corrupted, sometimes this statement will have exception
            try:
                cursor.execute(create_table)
            except Exception, e:
                print str(e)
                truncate_table = "TRUNCATE TABLE %s" % table_name
                cursor.execute(truncate_table)
        else:
            truncate_table = "TRUNCATE TABLE %s" % table_name
            cursor.execute(truncate_table)

        # begin to process data
        cur_poster = poster_name.encode('utf8')
        if '"' in cur_poster:
            cur_poster = cur_poster.replace('"', '\\"')
        if "'" in cur_poster:
            cur_poster = cur_poster.replace("'", "\\'")
        print 'The poster name is %s' % cur_poster

        # put the id and name of the poster into table
        temp = original_poster_id[0].split('/')
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

        # get information of following comments in first page
        ids = []
        names = []
        s = set()  # compute the num of comments of this post from unique id
        original_names = tree.xpath("//*[@class='topic-reply']/li[@class='clearfix comment-item']/div/a/img/@alt")
        original_ids = tree.xpath("//*[@class='topic-reply']//div[@class='user-face']/a/@href")

        for id in original_ids:
            temp = id.split('/')
            real_id = temp[len(temp) - 2]
            ids.append(real_id)
            s.add(real_id)

        for name in original_names:
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
                cur_url = self.current_url + "?start=%d" % start
                start += 100
                count = count + 1
                get_data = get_data_from_url.GetDataFromUrl(cur_url)
                content = get_data.get_data()
                # check if open the url successfully
                if content == "No":
                    continue
                html = content.decode('utf8')
                tree = etree.HTML(html)
                ids = []
                names = []
                original_names = tree.xpath("//*[@class='topic-reply']/li[@class='clearfix comment-item']/div/a/img/"
                                            "@alt")
                original_ids = tree.xpath("//*[@class='topic-reply']//div[@class='user-face']/a/@href")
                if len(original_names) == 0:
                    continue
                for id in original_ids:
                    temp = id.split('/')
                    real_id = temp[len(temp) - 2]
                    ids.append(real_id)
                    s.add(real_id)

                for name in original_names:
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
