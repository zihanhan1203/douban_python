import urllib2
import time


class GetDataFromUrl(object):
    def __init__(self, url):
        self.url = url

    def get_data(self):
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
        request = urllib2.Request(self.url, None, headers=headers)
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
                    return "No"
                else:
                    if i > 0:
                        set_of_failed_ip.append(res_ip[i - 1])
                    print 'RETRY!!!'
                    proxies['https'] = res_ip[i].strip('\n')
                    opener = urllib2.build_opener(urllib2.ProxyHandler(proxies))
                    time.sleep(0.5)
            else:
                if i > 0:
                    the_index_of_useful_ip = i - 1
                print 'Connect successfully!!!'
                time.sleep(0.1)
                break
        # put the proxy ips which cannot create valid connection in the end of the file to save time
        for i in range(the_index_of_useful_ip, 50):
            file_write_ob.write(res_ip[i])
        for i in set_of_failed_ip:
            file_write_ob.write(i)
        file_write_ob.close()
        return content
