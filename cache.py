import socket
import logging
import threading
import argparse
import sys
import os
import select
import traceback
import Queue
import heapq
import shutil
import time


class clientHandler(threading.Thread):
    def __init__(self, clientSocket, address, logger, queue, cacheList, folderName, connNum, maxConn, serverPort = 80):
        super(clientHandler, self).__init__()
        self.clientSocket = clientSocket
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientAddress = address
        self.logger = logger
        self.queue = queue
        self.folderName = folderName 
        self.cacheList = cacheList
        self.connNum = connNum
        self.maxConn = maxConn

        self.logString = ''
        self.clientHeaders = ''
        self.clientBody = ''
        self.clientVars = dict()
        self.url = ''
        self.path =''
        self.command =''
        self.queryString = ''
        self.version =''
        self.origUserAgent = ''
        self.clientFirstLine = ''
        self.startTime = ''
        self.elapsedTime = 0
        
        self.serverStatusLine = ''
        self.serverPort = serverPort # The port at which to contact the server
        self.serverBody = ''
        self.serverHeaders = ''
        self.serverVars = dict()
        self.serverStatusCode = 404 # Assume error until success
        self.serverStatusMessage = ''
        self.serverVersionResponse = ''
        self.success = False

        self.responseType = ''
        self.responseSize = 0


    def run(self):
        try:
            self.startTime = time.time()
            self.logString += "[Conn: {}/{} [Cache: {:.2f}/{}MB] [Items: {}]\n\n".format(self.connNum, self.maxConn, self.cacheList.tSize/(1024*1024.0), self.cacheList.maxSize/(1024*1024), self.cacheList.numItems())
            self.logString += '[CLI connected to {}:{}]\n'.format(self.clientAddress[0], self.clientAddress[1])
            cTime = time.localtime(time.time())
            self.logString += "[CLI ==> PRX ---SRV] @ {}:{}:{}\n".format(cTime.tm_hour, cTime.tm_min, cTime.tm_sec)

            # read request line and headers
            self.read_data(self.clientSocket)

            self.success = self.processRequestData()
            successCache = False

            if(self.success):
                self.logString += '\t> {}\n'.format(self.clientFirstLine)
                if 'User-Agent' in self.clientVars:
                    self.logString += '\t> {}\n'.format(self.origUserAgent)
                self.logString += '[SRV connected to {}:{}]\n'.format(self.url, self.serverPort)

            	cached = self.cacheList.searchUrl(self.url+self.path)
                if cached is not None:
                    self.logString += "@@@@@@@@@@@@@@@@@@ CACHE HIT @@@@@@@@@@@@@@@@@@@@\n"
                    successCache = self.sendCached(cached)
                    
                if cached is None or not successCache: # If there is no cache or if sending the cache fails, send request to remote server
                    self.logString += "################## CACHE MISS ###################\n"
                    self.sendRequest()
                    (data, success) = self.tunnelData()
                    if(success and self.queryString is ""): #disallow query strings because filenames with ? are not allowed
                        nCFile = cachedFile(self.url+self.path, self.folderName, data, self.logger)

                        self.logString += self.cacheList.append(nCFile)

                
        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.debug("Exception: {}".format(e))

        finally:
            self.logger.debug("Closing connection...")
            self.clientSocket.close()
            self.serverSocket.close()
            request = dict()
            request['request'] = 'reduce_conn_num'
            self.elapsedTime = time.time() - self.startTime
            self.logString += "# {}ms\n".format(self.elapsedTime)
            self.logString += "[CLI disconnected]\n"
            self.logString+="[SRV disconnected]\n"
            self.queue.put(request)
            if self.success:
                self.logger.info(self.logString)
    

    def read_data(self, tSocket):
        data= ""
        self.clientHeaders =""
        self.body = ""
        inputs = [tSocket,]

        # read in all data
        self.logger.debug("Reading data...")
        try:
            while True:
                # Use Select to test whether data is readable with a 0.1 seconds timeout
                readable, _, _ = select.select(inputs, [], [], 0.2)
                if (len(readable) > 0):
                    #self.logger.debug("Found readable data...")
                    tempData = tSocket.recv(1024)
                    data += tempData
                    if not tempData:
                        break
                else:
                    break

        except socket.error as e:
            self.logger.debug("Socket Error: {}".format(e))
            pass
        finally:
            # Parse data to separate into headers and body
            self.logger.debug("Parsing data...")
            if len(data) > 0:
                data_lines = data.splitlines(True) # True to keep ends
                header_end = 0
                for x in range(0, len(data_lines)):
                    if data_lines[x] == "\r\n":
                        header_end = x
                        break  # Break in case there is another line like this in the body

                self.clientHeaders = data_lines[0:header_end]
                if(len(data_lines) > header_end):
                    self.body = data_lines[header_end+1:]


    def processRequestData(self):
        success = True
        try:
            self.clientFirstLine = self.clientHeaders[0]
            self.logger.debug("Request line: {}".format(self.clientFirstLine))
            command, fullURL, version = self.clientFirstLine.split()
            self.command= command
            self.version= version.rstrip("\r\n")
            
            # Remove http://
            splitURL = fullURL.split('//',1)

            if len(splitURL) > 1:
                splitURL = splitURL[1]
            else:
                splitURL= splitURL[0]


            # Split path from url
            splitURL = splitURL.split('/',1)

            if len(splitURL) == 2:
                self.path = '/'+splitURL[1]
                splitURL = splitURL[0]
            else:
                splitURL = splitURL[0]
                self.path= "/"


            # Extract query string
            if '?' in self.path:
                splitPath = self.path.split('?')
                self.queryString = splitPath[-1]
                self.path = splitPath[0]


            # Obtain port if specific port is requested
            splitURL = splitURL.split(':',1)


            if len(splitURL) == 2:
                self.url = splitURL[0]
                self.serverPort = int(splitURL[1])
            else:
                self.url = splitURL[0]

            if self.url.endswith('/'):
                self.url = self.url[0:len(self.url)-2]

            request = ''
            

            if self.queryString != "":
                self.path = self.path + '?'+self.queryString # Readd query string to pass it to server

            if request != '':
                self.queue.put(request)
            
            #Process Headers
            for line in self.clientHeaders[1:]:
                if line is not '':
                    headerBuffer = line.split(":",1)[0]
                    headerBuffer = headerBuffer.strip("\r\n ") # remove extra spaces
                    self.clientVars[headerBuffer] = line.split(":",1)[1].strip("\r\n ") # Gets the header value after the colon

            # Prevents keep-alive connection
            self.clientVars["Connection"] = "close"

            if "Accept-Encoding" in self.clientVars:
                del self.clientVars["Accept-Encoding"]


            if 'User-Agent' in self.clientVars:
                self.origUserAgent = self.clientVars['User-Agent']

        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.debug("Error:{}".format(e))
            success = False
        finally:
            return success

    def processResponseData(self):
        success = True
        try:
            self.serverStatusLine = self.serverHeaders[0]
            splitStatus = self.serverStatusLine.split()
            self.serverVersionResponse = splitStatus[0]
            self.serverStatusCode = splitStatus[1]
            self.serverStatusMessage = " ".join(splitStatus[2:],)
            self.serverVersionResponse = splitStatus[-1]

            for line in self.serverHeaders[1:]:
                if line.rstrip("\r\n") is not '':
                    headerBuffer = line.split(":",1)[0]
                    headerBuffer = headerBuffer.strip("\r\n ") # remove extra spaces
                    self.serverVars[headerBuffer] = line.split(":",1)[1].strip("\r\n ")

            if "Content-Type" in self.serverVars:
                self.responseType = self.serverVars['Content-Type']

            if "Content-Length" in self.serverVars:
                self.responseSize = self.serverVars['Content-Length']
            else:
                self.responseSize = len(self.serverBody)


        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.debug("Error:{}".format(e))
            success = False
        finally:
            return success

    def sendRequest(self):
        try:
            addr = socket.gethostbyname(self.url)
            self.logger.debug("URL:{}\nAddress:{}".format(self.url, addr))
            self.serverSocket.connect((addr,self.serverPort))
            cTime = time.localtime(time.time())
            self.logString += "[CLI ---PRX ==> SRV] @ {}:{}:{}\n".format(cTime.tm_hour, cTime.tm_min, cTime.tm_sec)
            self.logString += '\t> {}\n'.format(self.clientFirstLine)
            if 'User-Agent' in self.clientVars:
                self.logString += '\t> {}\n'.format(self.origUserAgent)

            requestLine = "{} {} {}".format(self.command,self.path,self.version)
            self.serverSocket.send("{}\r\n".format(requestLine))


            for key, value in self.clientVars.items():
                self.serverSocket.send("{}: {}\r\n".format(key,value))

            self.serverSocket.send("\r\n")

            for line in self.clientBody:
                if line != "":
                    self.serverSocket.send(line)


        except socket.error as e:
            self.logger.debug(traceback.format_exc())
            self.logger.debug(e)
            self.handle_error(404)
        except Exception as e:
            self.logger.debug(e)

    def tunnelData(self):
        self.logger.debug("Tunneling data...")
        data= ""
        iteration = 0
        inputs = [self.serverSocket,]

        cTime = time.localtime(time.time())
        self.logString += "[CLI ---PRX <== SRV] @ {}:{}:{}\n".format(cTime.tm_hour, cTime.tm_min, cTime.tm_sec)

        # read in all data
        self.logger.debug("Reading data...")
        
        try:
            while True:
                # Use Select to test whether data is readable with a 1.0 seconds timeout
                readable, _, _ = select.select(inputs, [], [], 2.0)
                if (len(readable) > 0):
                    #self.logger.debug("Found readable data...")
                    tempData = self.serverSocket.recv(512)
                    data += tempData
                    if not tempData:
                        break
                    else:
                        self.clientSocket.send(tempData)
                else:
                    break

        except socket.error as e:
            self.logger.debug("Socket Error: {}".format(e))
            pass
        except Exception as e:
            self.logger.debug("Error: {}".format(e))
        finally:
            # Parse data to separate into headers and body
            self.logger.debug("Parsing data...")
            success = False
            if len(data) > 0:
                data_lines = data.splitlines(True) # True to keep ends
                header_end = 0
                for x in range(0, len(data_lines)):
                    if data_lines[x] == "\r\n":
                        header_end = x
                        break


                self.serverHeaders = data_lines[0:header_end]
                if(len(data_lines) > header_end):
                    self.serverBody = data_lines[header_end+1:]
            success = self.processResponseData()
            if(success):
                try:
                    self.logString += '\t> {} {}\n'.format(self.serverStatusCode, self.serverStatusMessage)
                    if(self.serverStatusCode == '200'):
                        self.logString += '\t> {} {}bytes\n'.format(self.responseType, self.responseSize)

                    cTime = time.localtime(time.time())
                    self.logString += "[CLI <== PRX ---SRV] @ {}:{}:{}\n".format(cTime.tm_hour, cTime.tm_min, cTime.tm_sec)
                    self.logString += '\t> {} {}\n'.format(self.serverStatusCode, self.serverStatusMessage)
                    if(self.serverStatusCode == '200'):
                        self.logString += '\t> {} {}bytes\n'.format(self.responseType, self.responseSize)
                except Exception:
                    self.success = False
            else:
                self.success = False

            return (data, success)

    
    def handle_error(self, errNum):
        if errNum == 404:
            self.clientSocket.send("{} 404 Not Found\r\n".format(self.version))
            self.clientSocket.send("Content-Type:text/html\r\n")
            body = '''<html><head><title>404 Not Found</title></head><body>
<h1>404 Not Found</h1>
<p>The requested URL {} was not found on this server.</p>
</body></html>'''.format(self.path)
            self.clientSocket.send("Content-Length:{}\r\n\r\n".format(len(body)))
            self.clientSocket.send(body)
        elif errNum == 403:
            self.clientSocket.send("{} 403 Forbidden\r\n".format(self.version))
            self.clientSocket.send("Content-Type: text/html\r\n")
            body = '''<html><head><title>Access Forbidden</title></head><body>
<h1>403 Forbidden</h1>
<p>You don't have permission to access the requested URL {}. There is either no index document or the directory is read-protected.</p>
</body></html>'''.format(self.path)
            self.clientSocket.send("Content-Length:{}\r\n\r\n".format(len(body)))
            self.clientSocket.send(body)
    
    def sendCached(self, cFile):
        success = False
        self.logger.debug("Sending cache of "+cFile.url)
        data = cFile.getData()
        if(data is not ""):
            self.clientSocket.send(data)

        if len(data) > 0:
            data_lines = data.splitlines(True) # True to keep ends
            header_end = 0
            for x in range(0, len(data_lines)):
                if data_lines[x] == "\r\n":
                    header_end = x
                    break


            self.serverHeaders = data_lines[0:header_end]
            if(len(data_lines) > header_end):
                self.serverBody = data_lines[header_end+1:]
        
        success = self.processResponseData()
        if(success):
            cTime = time.localtime(time.time())
            self.logString += "[CLI <== PRX ---SRV] @ {}:{}:{}\n".format(cTime.tm_hour, cTime.tm_min, cTime.tm_sec)
            self.logString += '\t> {} {}\n'.format(self.serverStatusCode, self.serverStatusMessage)
            if(self.serverStatusCode == '200'):
                self.logString+= '\t> {} {}bytes\n'.format(self.responseType, self.responseSize)
        
        return success


class ProxyServer():
    def __init__(self, port, logger, maxConn, maxSize, folderName="cached",address="0.0.0.0"):
        self.port = port
        self.address = address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.logger = logger
        self.maxConn = maxConn
        self.maxSize = maxSize
        self.folderName = folderName
        self.connNum = 0
        
        # Remove any files to start from scratch
        if(os.path.isdir(self.folderName)):
        		shutil.rmtree("cached",ignore_errors=True)
        
        os.mkdir(self.folderName);

        self.requestQueue = Queue.Queue()
        self.threadsQueue = Queue.Queue()
        self.cachedSize = 0
        self.cacheList = cacheSpecializedList(self.maxSize, self.logger)

    def runServer(self):
        try:
            self.socket.bind((self.address, self.port))
            self.socket.listen(0)
            print("Listening on port {} ...".format(self.port))
        except socket.error as e:
            self.logger.error('Could not listen on {}:{}: {}'.format(self.address, self.port, e))
            sys.exit(1)
        
        while True:
            # Accept connection
            clientSocket, address = self.socket.accept()
            try:
                # Create new handler instance, which is a thread object, and start. 
                #Server will continue accepting connections to allow parallel connection
                if(self.connNum < self.maxConn or self.maxConn == 0):
                    handleInstance = clientHandler(clientSocket, address, self.logger, self.requestQueue, self.cacheList, self.folderName, self.connNum, self.maxConn)
                    handleInstance.start()
                    self.connNum += 1
                else:
                    time.sleep(0.1)

                
                # Each client has the ability to submit to this queue. Allowing them to modify the
                # The actions of the server as a whole
                self.checkRequestsQueue()
            except Exception as e:
                self.logger.debug("Exception: {}".format(e))
                sys.exit(1)
    def checkRequestsQueue(self):
        try:
            while(not self.requestQueue.empty()):
                request = self.requestQueue.get()
                
                if request['request'] == "reduce_conn_num":
                    self.connNum -= 1
                

        except Queue.Empty:
            pass


# This class allows a user to use multiple loggers with different levels using a single object
class superLogger():
    def __init__(self, errorLogger, debugLogger, infoLogger, loggerLvl=logging.INFO):
        self.errorLogger = errorLogger
        self.debugLogger = debugLogger
        self.infoLogger = infoLogger
        self.loggerLvl = loggerLvl
        
        self.errorLogger.setLevel(loggerLvl)
        self.debugLogger.setLevel(loggerLvl)
        self.infoLogger.setLevel(loggerLvl)
        
    def info(self,text):
        self.infoLogger.info(text)
    def debug(self, text):
        self.debugLogger.debug(text)
    def error(self, text):
        self.errorLogger.error(text)
    def setLevel(self, loggerLvl):
        self.loggerLvl = loggerLvl
        self.errorLogger.setLevel(loggerLvl)
        self.debugLogger.setLevel(loggerLvl)
        self.infoLogger.setLevel(loggerLvl)

class myStream():
    def __init__(self):
        self.count = 1
    def write(self, text):
        print'\r-----------------------------------------------'
        print('{} {}'.format(self.count, text)),
        self.count += 1
    def flush(self):
        pass

class cachedFile():
    def __init__(self, url, rootFolder, data, logger, rank = 1):
        if(data == ''):
            self.rank = -1
            return

        self.logger = logger
        self.file = None
        self.size = 0
        self.rank =rank
        self.rootFolder = './'
        self.fileName = ''
        self.midFolders = ''
        self.url = url.strip('/')
        self.fullDirectories = ''
        self.fullPath = ''
        self.valid = True

        if rootFolder is not '':
            self.rootFolder+= rootFolder.strip('/')+'/'

        self.createDirs()
        	
        self.writeToFile(data)

    def createDirs(self):
        try:
            filePath = self.url

            filePathSplit = filePath.split('/')

            self.fullDirectories += self.rootFolder

            # Analyze URL and create directories
            if(len(filePathSplit) > 1):
                for i in range(len(filePathSplit)-1):
                    self.midFolders+= (filePathSplit[i]+'/')

            self.fileName = filePathSplit[-1]

            self.logger.debug("Creating Cached file with url "+self.url)

            if(self.midFolders is ''): # In this case there is a blank path, e.g, webpage www.example.com would be stored in www.example.com/www.example.com
                self.midFolders = self.url+'/'

            self.fullDirectories += self.midFolders

            if not os.path.isdir(self.fullDirectories):
                os.makedirs(self.fullDirectories)

            self.fullPath = self.fullDirectories+self.fileName

            while(os.path.isdir(self.fullPath)):
                self.fullDirectories = self.fullPath +'/'
                self.fullPath += ('/'+ self.fileName)

        except Exception as e:
            self.valid = False
            self.logger.debug(e)


    def writeToFile(self, data):
        try:
            self.file = open(self.fullPath, 'w')
            self.file.write(data)
            self.file.close()
            
            self.size = len(data)
        except Exception as e:
            self.valid = False
            self.logger.debug(traceback.format_exc())
            self.logger.debug("Error:{}".format(e))
        
    def getData(self):
        try:
            data = ''
            self.logger.debug("Sending data from filename " +self.fullPath)
            with open(self.fullPath, 'r') as cFile: # Automatically closes file on exit
                data += cFile.read()

            return data
        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.debug("Error:{}".format(e))
            self.valid = False
            return ""
    def deleteFile(self):
        if(os.path.isfile(self.fullPath)):
            os.remove(self.fullPath)
        self.deleteEmptyFolders(self.fullDirectories)

    def deleteEmptyFolders(self, path):
        if not os.path.isdir(path):
            return
        else:
            parentDir = os.path.abspath(os.path.join(path, os.pardir))

        # remove empty subfolders
        files = os.listdir(path)
        
        if len(files) == 0:
            os.rmdir(path)
            self.deleteEmptyFolders(parentDir)

class cacheSpecializedList():
    def __init__(self, maxSize, logger):
        self.tSize = 0
        self.cacheList = list()
        self.maxSize = maxSize
        self.logger = logger
        self.largestRank = 0
    
    def append(self, cFile):
        # Appends new cache file to list of caches
        # Returns formed String to be added to log
        if cFile.size > self.maxSize:
            return # Cant accept file because it is too big
        if not cFile.valid:
            cFile.deleteFile()
            return #cached File not valid

        logString = ''

        if(self.maxSize is not 0):
            while(self.tSize + cFile.size > self.maxSize):
                popped = self.getSmallest()
                popped.deleteFile()

                logString += "################# CACHE REMOVED #################\n"
                cTime = time.localtime(time.time())
                logString += "\t> {} {:.2f}MB @{}:{}:{}\n".format(popped.fullPath, popped.size/(1024.0*1024),cTime.tm_hour, cTime.tm_min, cTime.tm_sec)
                logString += "\t> This file has been removed due to LRU !\n"
                self.smallestRank = popped.rank + 1
                self.tSize -= popped.size
		
        self.tSize += cFile.size
        self.logger.debug('New size of cache is {} bytes\n'.format(self.tSize))
        cFile.rank = self.largestRank
        self.largestRank += 1
        heapq.heappush(self.cacheList, (cFile.rank, cFile))
        logString += "################## CACHE ADDED ##################\n"
        cTime = time.localtime(time.time())
        logString += "\t> {} {:.2f}MB @{}:{}:{}\n".format(cFile.fullPath, cFile.size/(1024.0*1024),cTime.tm_hour, cTime.tm_min, cTime.tm_sec)
        logString += "\t> This file has been added to the cache\n"
        logString += "#################################################\n"

        return logString

    def getSmallest(self):
        smallest = heapq.heappop(self.cacheList)
        return smallest[1]

    def searchUrl(self, url):
        found = None

        url = url.strip('/')
        if url == '':
            return None

        for cFile in self.cacheList:
            if(cFile[1].url == url):
                found = cFile[1]
        if found is None:
            return None

        if not found.valid:
            self.removeFile(found)
            return None
        else:
            return found

    def removeFile(self, cFile):
        self.cacheList.remove(cFile)
        self.tSize -= cFile.size
        cFile.deleteFile()
        heapq.heapify(self.cacheList)

    def updateRank(self, cFile):
        found = None

        for cacheFile in self.cacheList:
            if cacheFile[1] == cFile:
                foundFile = cacheFile
		
        if(found is not None):
            self.cacheList.remove(cacheFile)
            heapq.heapify(self.cacheList)
            cFile.rank = self.largestRank
            self.largestRank += 1
            heapq.heappush(self.cacheList, (cFile.rank, cFile))
    def numItems(self):
        return len(self.cacheList)
			
			
    
                
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('port',type=int, help="Port number for server to listen", nargs='?',default=80)
    parser.add_argument('maxConn',type=int, help="Maximum Connection Count Concurrently Working (0 for infinite)", nargs='?',default=10)
    parser.add_argument('maxSize',type=int, help="Maximum size of Cache store in MiB (0 for infinite)", nargs='?',default=10)
    parser.add_argument("-v", dest="verbose", help="Set logging to DEBUG level", action="store_true")

    args = parser.parse_args()


    if args.verbose:
        LOGLEVEL = logging.DEBUG
    else:
        LOGLEVEL = logging.INFO

    log_formatter2 = logging.Formatter(
        fmt  = '[%(asctime)s][%(levelname)s][%(threadName)s][%(name)s]:%(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
    )
    
    logger1 = logging.getLogger('INFO-Logger')
    logger2 = logging.getLogger('DEBUG/ERROR-Logger')

    stream = myStream()
    strmHandler1 = logging.StreamHandler(stream)
    strmHandler2 = logging.StreamHandler(sys.stdout)

    strmHandler2.setFormatter(log_formatter2)

    logger1.addHandler(strmHandler1)
    logger2.addHandler(strmHandler2)

    sLogger = superLogger(logger2, logger2, logger1, LOGLEVEL)


    server = ProxyServer(args.port, sLogger, args.maxConn, args.maxSize * 1024*1024)
    try:
        server.runServer()
    except KeyboardInterrupt:
        sys.exit(1)
