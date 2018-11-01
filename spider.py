#!/usr/bin/python
# -*- coding: utf-8 -*-
import get_data_from_url
from lxml import etree
import comment_spider


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
        set_of_post = set()  # ATTENTION the way to create empty set
        get_data = get_data_from_url.GetDataFromUrl(self.topic_url)
        content = get_data.get_data()
        html = content.decode('utf8')
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
