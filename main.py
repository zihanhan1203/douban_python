#!/usr/bin/python
# -*- coding: utf-8 -*-


import spider
import pytz
import datetime
import pymysql
import time

if __name__ == '__main__':
    run_main_function = True
    if run_main_function:
        tz = pytz.timezone('Asia/Shanghai')  # Attention TIP!!! HOW TO PROCESS TIMEZONE
        pre_day = 3
        set_of_url = set()  # To restore each day's urls of the posts
        if_today_table_is_create = False
        times = 1
        today_table_name = "2018_11_03"
        if_restart = True
        while True:
            # connect to db
            db = pymysql.connect("localhost", "root", "123456", "douban_rocketgirl101_group")
            cursor = db.cursor()
            now_time = datetime.datetime.now(tz)
            # if it is a new day, put the data of todaytable into base_data, and fix the value of today_table_name
            # and clear the set_of_url
            if now_time.day != pre_day:
                get_data = "SELECT * FROM %s" % today_table_name
                cursor.execute(get_data)
                datas = cursor.fetchall()
                for data in datas:
                    id = data[0]
                    name = data[1]
                    if '"' in name:
                        name = name.replace('"', '\\"')
                    elif "'" in name:
                        name = name.replace("'", "\\'")
                    post = data[2]
                    comment = data[3]
                    comment_got = data[4]
                    put_to_base_data = "INSERT INTO base_data(ID,name,post,comment,comment_got)" \
                                       " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                                       " ON DUPLICATE KEY UPDATE name = '%s'," \
                                       "post=post+%d, comment=comment+%d, " \
                                       "comment_got=comment_got+%d" % (
                                           id, name, post, comment, comment_got,
                                           name, post, comment, comment_got)
                    try:
                        cursor.execute(put_to_base_data)
                    except Exception, e:
                        name = "The_name_has_problem"
                        put_to_base_data = "INSERT INTO base_data(ID,name,post,comment,comment_got)" \
                                           " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                                           " ON DUPLICATE KEY UPDATE name = '%s'," \
                                           "post=post+%d, comment=comment+%d, " \
                                           "comment_got=comment_got+%d" % (
                                               id, name, post, comment, comment_got,
                                               name, post, comment, comment_got)
                        cursor.execute(put_to_base_data)

                # fix the value of today_table_name and create today table
                today_table_name = now_time.strftime("%Y_%m_%d")
                create_to_day_table = "CREATE table %s like base_data" % today_table_name
                cursor.execute(create_to_day_table)
                db.commit()
                # delete temp table
                drop_table = "SELECT CONCAT('DROP TABLE ',table_name, ';') FROM information_schema.tables " \
                             "Where table_name LIKE 'temp_%'"
                cursor.execute(drop_table)
                res = cursor.fetchall()
                for r in res:
                    cursor.execute(r[0])
                db.commit()
                print "temp tables are cleared"

                # clear the set set_of_url
                set_of_url.clear()
                times = 1
                pre_day = now_time.day

            print 'Day:' + today_table_name + 'NO.%d time START!' % times
            times += 1
            # if main is interrupted, we need to put the url checked into the set
            if if_restart:
                file_path = './result/' + today_table_name + '.txt'
                file_object = open(file_path, 'r')
                checked_urls = file_object.readlines()
                file_object.close()
                for checked_url in checked_urls:
                    set_of_url.add(checked_url.strip('\n'))

            # if restarted, put the data into today's table
            if if_restart:
                if_restart = False
                if_today_table_is_create = True
                truncate_today_table = "TRUNCATE TABLE %s" % today_table_name
                cursor.execute(truncate_today_table)
                for post_id in set_of_url:
                    temp = post_id.split('/')
                    real_post_id = temp[len(temp) - 2]
                    table_name = 'temp_' + real_post_id
                    get_data = "SELECT * FROM %s" % table_name
                    cursor.execute(get_data)
                    datas = cursor.fetchall()
                    for data in datas:
                        id = data[0]
                        name = data[1]
                        if '"' in name:
                            name = name.replace('"', '\\"')
                        elif "'" in name:
                            name = name.replace("'", "\\'")
                        post = data[2]
                        comment = data[3]
                        comment_got = data[4]
                        put_data = "INSERT INTO %s(ID,name,post,comment,comment_got)" \
                                   " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                                   " ON DUPLICATE KEY UPDATE name = '%s', " \
                                   "post=post+%d, comment=comment+%d, " \
                                   "comment_got=comment_got+%d" % (
                                       today_table_name, id, name, post, comment, comment_got,
                                       name, post, comment, comment_got)
                        try:
                            cursor.execute(put_data)
                        except Exception, e:
                            name = "The_name_has_problem"
                            put_data = "INSERT INTO %s(ID,name,post,comment,comment_got)" \
                                       " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                                       " ON DUPLICATE KEY UPDATE name = '%s', " \
                                       "post=post+%d, comment=comment+%d, " \
                                       "comment_got=comment_got+%d" % (
                                           today_table_name, id, name, post, comment, comment_got,
                                           name, post, comment, comment_got)
                            cursor.execute(put_data)

                # data of today's active people and posts
                get_num_of_people = "SELECT COUNT(ID) from %s" % today_table_name
                cursor.execute(get_num_of_people)
                num_of_people = cursor.fetchall()[0][0]
                num_of_post = len(set_of_url)
                put_data_to_eachday = "INSERT INTO eachday_groupdata(date, posts, people) VALUES ('%s','%d', '%d')" \
                                      "ON DUPLICATE KEY UPDATE posts=%d, people=%d" % (today_table_name, num_of_post,
                                                                                       num_of_people, num_of_post,
                                                                                       num_of_people)
                cursor.execute(put_data_to_eachday)
                db.commit()
            elif not if_today_table_is_create:
                # create today table
                create_to_day_table = "CREATE table %s like base_data" % today_table_name
                cursor.execute(create_to_day_table)
                db.commit()
                if_today_table_is_create = True

            # create the spider object which used to read the front 125 posts' urls
            getToPage = spider.PageSpider()
            current_page = 0

            # ATTENTION TIP !!! str.strip('-+=') can delete all the '-' '+' '=' in the string

            # set the interval
            interval = 30 * 60
            if 3 <= now_time.hour < 8:
                interval = 5 * 60 * 60
            while current_page <= 125:
                set_of_url = set_of_url | getToPage.start(current_page, set_of_url, today_table_name)
                current_page += 25

            # truncate today table to add the new content
            truncate_today_table = "TRUNCATE TABLE %s" % today_table_name
            cursor.execute(truncate_today_table)

            # put data into today table
            for post_id in set_of_url:
                temp = post_id.split('/')
                real_post_id = temp[len(temp) - 2]
                table_name = 'temp_' + real_post_id
                get_data = "SELECT * FROM %s" % table_name
                cursor.execute(get_data)
                datas = cursor.fetchall()
                for data in datas:
                    id = data[0]
                    name = data[1]
                    if '"' in name:
                        name = name.replace('"', '\\"')
                    elif "'" in name:
                        name = name.replace("'", "\\'")
                    post = data[2]
                    comment = data[3]
                    comment_got = data[4]
                    put_data = "INSERT INTO %s(ID,name,post,comment,comment_got)" \
                               " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                               " ON DUPLICATE KEY UPDATE name = '%s', " \
                               "post=post+%d, comment=comment+%d, " \
                               "comment_got=comment_got+%d" % (
                                   today_table_name, id, name, post, comment, comment_got,
                                   name, post, comment, comment_got)
                    try:
                        cursor.execute(put_data)
                    except Exception, e:
                        name = "The_name_has_problem"
                        put_data = "INSERT INTO %s(ID,name,post,comment,comment_got)" \
                                   " VALUES ('%s', '%s', '%d', '%d', '%d')" \
                                   " ON DUPLICATE KEY UPDATE name = '%s', " \
                                   "post=post+%d, comment=comment+%d, " \
                                   "comment_got=comment_got+%d" % (
                                       today_table_name, id, name, post, comment, comment_got,
                                       name, post, comment, comment_got)
                        cursor.execute(put_data)

            get_num_of_people = "SELECT COUNT(ID) from %s" % today_table_name
            cursor.execute(get_num_of_people)
            num_of_people = cursor.fetchall()[0][0]
            num_of_post = len(set_of_url)
            put_data_to_eachday = "INSERT INTO eachday_groupdata(date, posts, people) VALUES ('%s','%d', '%d')" \
                                  "ON DUPLICATE KEY UPDATE posts=%d, people=%d" % (today_table_name, num_of_post,
                                                                                   num_of_people, num_of_post,
                                                                                   num_of_people)
            cursor.execute(put_data_to_eachday)
            db.commit()
            cursor.close()
            db.close()
            new_now_time = datetime.datetime.now(tz)
            print new_now_time
            interval = interval - (time.mktime(new_now_time.timetuple()) - time.mktime(now_time.timetuple()))
            if interval < 0:
                interval = 0
            print "Starting to sleep for %d seconds" % interval
            time.sleep(interval)
