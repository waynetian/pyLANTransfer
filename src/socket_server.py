import socket
import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
#'a'




class ThreadUDPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        '''
        Process Request
        '''
        print '*********1'
        try:
            data = self.request[0].strip()
        except Exception,e:
            print 'udp recv error1', e
        #print 'udp recv', data
        print '**********2'
        socket = self.request[1]
        localIP, localPort = socket.getsockname()
        ip,      port      = self.client_address
        print '***********3'
        if localIP == ip:
            return 
        try:
            item = data.split('|')
            if item[0] == 'sohudrive':
                cmd = item[1]
                if cmd == 'login':
                    print 'recv login', self.client_address
                    ip, port = self.client_address
                    #print dir(self)
                    self.server.addIPList(ip)
                    msg = "sohudrive|loginack"
                    socket.sendto(msg, self.client_address)
                elif cmd == 'loginack':
                    print 'recv loginack', self.client_address
                    ip, port = self.client_address
                    self.server.addIPList(ip)
                elif cmd == 'query':
                    print 'recv query', self.client_address
                    resourceID = item[2]
                    eTag       = item[3]
                    ret        = self.doQuery(resourceID, eTag)
                    msg = ''
                    if ret == True:
                        msg = "sohudrive|queryack|yes|%s|%s" %(resourceID, eTag)
                    else:
                        msg = "sohudrive|queryack|no|%s|%s" %(resourceID, eTag)
                    socket.sendto(msg, self.client_address)
                elif cmd == 'queryack':
                    ret        = item[2]
                    resourceID = item[3]
                    eTag       = item[4]
                    ip, port   = self.client_address
                    if ret == 'yes':
                        key = '%s|%s' %(resourceID, eTag)
                        self.server.addQueryResult(key, ip)

        except Exception, e:
            print 'udp recv error', e

    def doQuery(self, resourceID, eTag):
        '''
        Process Query Function
        '''
        return True



class ThreadUDPServer(SocketServer.ThreadingMixIn, SocketServer.UDPServer):
    def init(self):
        self.lstIP       = []
        self.lstSpecIP   = []
        self.mapQueryAck = {}

    def resetQueryResult(self, resourceID, ip):
        # lock
        self.mapQueryAck = {}

    def addQueryResult(self, key, ip):
        # lock
        if key not in self.mapQueryAck:
            self.mapQueryAck[key] = [ip]
        else:
            if ip not in self.mapQueryAck[key]:
                self.mapQueryAck[key].append(ip)

    def getQueryResult(self, key):
        # lock
        if key in self.mapQueryAck:
            lstIP = self.mapQueryAck[key]
            return lstIP
        return None


    def addIPList(self, ip):
        if ip not in self.lstIP:
            self.lstIP.append(ip)



class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print 'tcp recv', data
        try:
            item = data.split('|')
            if item[0] == 'sohudrive':
                cmd = item[1]
                print item[2]
                print item[3]
                print item[4]
                
                if cmd == 'fetch':
                    print 'aaa0'
                    resourceID = item[2]
                    eTag       = item[3]
                    offset     = int(item[4])
                    fileName   = 'test_tcp.txt'
                    bufsize    = 1024*1024
                    print 'aaa01'
                    f = open(fileName, 'rb')
                    print 'aaa02'
                    import os
                    f.seek(offset, os.SEEK_SET)
                    print 'aaa1'
                    while True:
                        print 'aaa2'
                        data = f.read(bufsize)
                        print data
                        if len(data) == 0:
                            break
                        self.request.sendall(data)
                    f.close()
        except Exception, e:
            print 'tcp error', e



class TCPServer(SocketServer.TCPServer):
    pass




class SohuServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port


    def createTCPServer(self):
        self.TCPServer = TCPServer((self.host, self.port), ThreadedTCPRequestHandler)
        self.TCPThread = threading.Thread(target=self.TCPServer.serve_forever)
        self.TCPThread.daemon = True


    def createUDPServer(self):
        self.UDPServer = ThreadUDPServer((self.host, self.port), \
                ThreadUDPRequestHandler)
        self.UDPServer.init()
        self.udpsock = self.UDPServer.socket
        self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        self.UDPThread = threading.Thread(target=self.UDPServer.serve_forever)
        self.UDPThread.daemon = True

    def initialize(self):
        address = ('255.255.255.255', self.port)
        msg = 'sohudrive|%s|%s' %('login', self.host)
        self.udpsock.sendto(msg, address)

        lstSpecIP = self.UDPServer.lstSpecIP
        for clientIP in lstSpecIP:
            address = (clientIP, self.port)
            self.udpsock.sendto(msg, address)
        
    def initSock(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)

    def addSpecIP(self, ip):
        if ip not in self.UDPServer.lstSpecIP:
            self.UDPServer.lstSpecIP.append(ip)


    def isExistInRemote(self, resourceID, eTag):
        lstIP = self.UDPServer.lstIP
        for client in lstIP:
            sock = self.initSock()
            sock.connect((client, self.port))
            msg = 'sohudrive|query|%s|%s' %(resourceID, eTag)
            socket.sendall(msg)
            data = sock.recv(1024)
            try:
                item = data.split('|')
                if item[0] == 'sohudrive':
                    cmd = item[1]
                    if cmd == 'queryack':
                        ret = item[2]
                        if ret == 'yes':
                            return (True, client)
            except Exception, e:
                print e
        return (False, '')

    def queryFileExist(self, resourceID, eTag):
        address = ('255.255.255.255', self.port)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        msg = 'sohudrive|%s|%s|%s' %('query', resourceID, eTag)
        self.UDPServer.lstQueryAck = []
        s.sendto(msg, address)

        lstSpecIP = self.UDPServer.lstSpecIP
        for clientIP in lstSpecIP:
            address = (clientIP, self.port)
            s.sendto(msg, address)

    def getQueryResult(self, resourceID, eTag):
        key = "%s|%s" %(resourceID, eTag)
        return self.UDPServer.getQueryResult(key)



    def fetchFile(self, currentSize, fileSize, fHandler, eTag, resourceID, clientIP):
        offset = currentSize
        sock = self.initSock()
        sock.connect((clientIP, self.port)) 
        msg = 'sohudrive|fetch|%s|%s|%d' %(resourceID, eTag, offset)
        socket.sendall(msg)

        while offset >= fileSize:
            data = sock.recv(1024*1024)
            if len(data) == 0:
                break
            fHandler.write(data)
            offset += len(data)

    def run(self):
        self.TCPThread.start()
        self.UDPThread.start()
        while True:
            import time
            time.sleep(1)


if __name__ == '__main__':
    s = SohuServer('10.7.4.2', 31502)
    s.createUDPServer()
    s.addSpecIP('10.7.255.255')           
    s.createTCPServer()
    s.initialize()
    s.run()

   


