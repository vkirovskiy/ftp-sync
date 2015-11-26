#!/usr/bin/env python

import sys
import os
from ftplib import FTP
import argparse
from multiprocessing import Pool, Queue

parser = argparse.ArgumentParser(prog = "sync-ftp-to.py")
parser.add_argument("--user", help="User name")
parser.add_argument("--passwd", help="Password")
parser.add_argument("-s", help="server name", required=True)
parser.add_argument("-n", help="number of threads to download", required=False)
parser.add_argument("lpath", help="Local path to sync")
parser.add_argument("rpath", help="Remote path to sync")

args = parser.parse_args()

if args.user: 
    USER=args.user

if args.passwd:
    PASS = args.passwd

if args.s:
    SRV = args.s

if args.lpath:
    SYNCPATH = args.lpath

if args.rpath:
    REMOTEROOT = args.rpath

localcharset = 'utf8'
remotecharset = 'cp1251'
nofthreads = 4

if args.n:
    nofthreads = args.n

listing = {}
templst = []

def ftp_lines_callback(data):
    global templst
    templst.append(data)

def is_dir(line):
    if line[0] == 'd':
        return 1
    else:
        return 0

def get_name(line):
    return line.split()[8:]

def get_size(line):
    return line.split()[4]

def get_file(ftpsock, rroot, item):
    global SYNCPATH
    global localcharset
    global remotecharset

    iname = item[0]
    isize = item[2]

    localpath = SYNCPATH + iname.decode(remotecharset).encode(localcharset)


    if not os.path.isdir(os.path.dirname(localpath)):
        os.makedirs(os.path.dirname(localpath))

    #print "Get " + item
    #print "Save to " + localpath
    if os.path.exists(localpath):
        if os.path.getsize(localpath) != int(isize):
            print "Size of " + localpath + " is different. Download it again. " + str(os.path.getsize(localpath)) +","+ isize
            os.remove(localpath)
        
    if os.path.exists(localpath) and os.path.getsize(localpath) == int(isize):
        return localpath + " and file on FTP are same"

    if not os.path.exists(localpath):
        ret = ftpsock.retrbinary("RETR " + rroot + iname, open(localpath, 'wb').write)
        return ret, localpath  

def walk_ftp(ftpsock, path, lst):
    global templst
    ftp.cwd(path)
    templst = []
    items = ftp.retrlines('LIST', ftp_lines_callback)
 
    for i in templst:
        filename = r" ".join(get_name(i))
        print filename, is_dir(i)
        if is_dir(i):
            lst[filename] = {}
            walk_ftp(ftpsock, path+'/'+filename, lst[filename])
        else:
            lst[filename] = get_size(i)
            #print get_file(ftpsock, path+'/'+filename)

def expand_list(lst, rootpath, q):
    for key,value in lst.iteritems():

        if isinstance(value, str):
            q.put([rootpath + '/' + key, 'file', value])
        if isinstance(value, dict) and len(value) > 0:
            q.put([rootpath + '/' + key, 'dir', 0])
            if len(value) > 0: 
                expand_list(lst[key], rootpath + '/' + key, q)

def downloader(user, passwd, server, queue, remoteroot):
    ftp = FTP(server)
    ftp.login(user, passwd)

    while True:
        if not queue.empty():
            item = queue.get(True)
            #print "Download " + item[0]
            if item[1] == 'file':
                print os.getpid(), get_file(ftp, remoteroot, item)
        else:
            break 

    ftp.quit()

if __name__ == '__main__':

    procqueue = Queue()

    ftp = FTP(SRV)                                        
    ftp.login(USER, PASS)
    listing = { '/': {}}
    walk_ftp(ftp, REMOTEROOT, listing['/'])
    ftp.quit()

    print listing
    expand_list(listing['/'], '', procqueue)

    print "Starting " + str(nofthreads) + " threads to download"
    p = Pool(nofthreads, downloader, (USER, PASS, SRV, procqueue, REMOTEROOT,))
    p.close()
    p.join()


    

    
