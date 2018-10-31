#!/usr/bin/python
# -*- coding: utf-8 -*-

import urllib2
from lxml import etree
import comment_spider
import time


class PageSpider(object):
    def __init__(self):
        self.topic_url = ''
        self.big_set = set()
        self.today_table_name = ''

    def start(self, current_page, big_set, today_table_name):
        print 'Start to get topics from page %d' % current_page  # %s represents string %d represents integer
        self.topic_url = "https://www.douban.com/group/639264/discussion?start=%d" % current_page
        self.big_set = big_set
        self.today_table_name = today_table_name
        return self.get_into_topics()

    def get_into_topics(self):
        # driver = webdriver.Chrome()
        # driver.get(self.topic_url)
        # proxies = {"https": "118.178.227.171:80", "http": "115.218.210.189:9000"}
        # s = requests.session()
        # s.keep_alive = False
        # response = requests.get(self.topic_url, proxies=proxies)
        # html = self.encode_html(response.text)
        # driver.close()
        # print html
        # httpproxy_handler = urllib2.ProxyHandler({"https": "118.178.227.171:80"})
        set_of_post = set()  # ATTENTION the way to create empty set
        proxies = {}
        file_path = './ip.txt'
        file_ob = open(file_path, 'r')
        res_ip = file_ob.readlines()  # get proxy ips
        file_ob.close()
        file_write_ob = open(file_path, 'w')
        set_of_failed_ip = []
        the_index_of_useful_ip = 0
        httpproxy_handler = urllib2.ProxyHandler({})
        headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'}
        request = urllib2.Request(self.topic_url, None, headers=headers)
        opener = urllib2.build_opener(httpproxy_handler)
        for i in range(51):
            try:
                print "Try to get the urls of the post in page %s" % self.topic_url
                response = opener.open(request, timeout=5)
                content = response.read()
                response.close()
            except Exception, e:
                if i >= 50:
                    print str(e)
                    for ip in res_ip:
                        file_write_ob.write(ip)
                    file_write_ob.close()
                    return set_of_post
                else:
                    if i > 0:
                        set_of_failed_ip.append(res_ip[i-1])
                    proxies['https'] = res_ip[i].strip('\n')  # change ip
                    opener = urllib2.build_opener(urllib2.ProxyHandler(proxies))
                    time.sleep(0.5)
            else:
                if i > 0:
                    the_index_of_useful_ip = i-1
                print "Success!!!"
                opener.close()
                time.sleep(0.1)
                break

        for i in range(the_index_of_useful_ip, 50):
            file_write_ob.write(res_ip[i])
        for i in set_of_failed_ip:
            file_write_ob.write(i)
        file_write_ob.close()

        html = self.encode_html(content)
        tree = etree.HTML(html)
        urls = tree.xpath("//*[@id='content']//tr/td[@class='title']/a[@href]/@href")
        fetch_each_comment = comment_spider.CommentSpider()

        # save current set of url
        file_path = './result/' + self.today_table_name + '.txt'
        # create a set to store the url of each post to judge if the post is scanned
        for each_url in urls:
            print 'The url of the post is %s' % each_url
            temp = each_url.split('/')
            url_id = temp[len(temp) - 2]
            if each_url in self.big_set:
                print 'The post is old'
                fetch_each_comment.start(each_url, True, url_id)
            else:
                print 'The post is new'
                if fetch_each_comment.start(each_url, False, url_id):
                    set_of_post.add(each_url)
                    file_object = open(file_path, 'a')
                    file_object.write(each_url + '\n')
                    file_object.close()
        return set_of_post

    def encode_html(self, html):
        return html.decode('utf8')
