#!/usr/bin/env python3

"""
Ten kod nie je este dokonceny, a je skaredy.
Pri spustani testov, na html stranke sa objavuju chyby, ze je nejaka syntaksicka chyba vo subore test.py, 
v adresaroch test01, test02, ...
Celkom funguje to dobre, ale otazka je, ci spravne nacitavam yaml subory zo adresara data,
a moze byt chyba v tich testoch, ak zle nacitavam yaml subory 

"""

import socket
import multiprocessing
import logging
import os
import yaml

STATUS_OK=(100,'OK')
STATUS_NO_SUCH_KEY=(200,'No such key')
STATUS_READ_ERROR=(201,'Read error')
STATUS_FILE_FORMAT_ERROR=(202,'File format error')
STATUS_UNKNOWN_METHOD=(203, "Unknown method")
STATUS_NO_SUCH_FIELD=(204,'No such field')
STATUS_BAD_REQUEST=(300,'Bad request')

class BadRequest(Exception):

    pass

class ConnectionClosed(Exception):

    pass

logging.basicConfig(level=logging.DEBUG)

def get_yaml(list_yaml, key):
    for item_yaml in list_yaml:
        if key in item_yaml:
            try:
                with open(f'data/{item_yaml}') as f:
                    return yaml.safe_load(f)
            except FileNotFoundError:
                return STATUS_NO_SUCH_KEY
            except OSError:
                return STATUS_READ_ERROR
            except yaml.error.YAMLError:
                return STATUS_FILE_FORMAT_ERROR
    return STATUS_NO_SUCH_FIELD 

def get_yaml_list():
    try:
        return os.listdir("data")
    except OSError as e:
        logging.error(f"Error listing files in 'data' directory: {e}")
        return STATUS_READ_ERROR

def method_KEYS(content):
    
    if(len(content)>0):
        return STATUS_BAD_REQUEST, []

    list_yaml = get_yaml_list()
    if list_yaml == STATUS_READ_ERROR:
        return list_yaml, []
    
    logging.info(f"Client requested list of yamls {list_yaml}")

    result = [f"{item.split('.')[0]}" for item in list_yaml]
    return STATUS_OK, yaml.dump(result)

def method_GET(content):

    if len(content) != 2:
        return STATUS_BAD_REQUEST, []
    
    list_yaml = get_yaml_list()
    if list_yaml == STATUS_READ_ERROR:
        return list_yaml, []
    
    chars_to_avoid = {" ", ":", "/"}
    split1 = content[0].split(":", 1)
    key=split1[1]
    split2 = content[1].split(":", 1)
    field = split2[1]

    if not split2[0] == "field" or not split1[0] == "key":
        return STATUS_BAD_REQUEST, []

    is_clear_key = all(char not in chars_to_avoid for char in key)
    if not is_clear_key:
        return STATUS_BAD_REQUEST, []
    
    diction=get_yaml(list_yaml, key)
    
    if isinstance(diction, dict):
        return STATUS_OK, yaml.dump(diction[field])
    else:
        return diction, []

def method_FIELDS(content):

    if(len(content)!=1):
        return STATUS_BAD_REQUEST, []

    list_yaml = get_yaml_list()
    if list_yaml == STATUS_READ_ERROR:
        return list_yaml, []

    split_key = content[0].split(":", 1)
    key=split_key[1]

    if not split_key[0] == "key":
        return STATUS_BAD_REQUEST, []

    chars_to_avoid = {" ", ":", "/"}
    is_clear_key = all(char not in chars_to_avoid for char in key)
    if not is_clear_key:
        return STATUS_BAD_REQUEST, []

    print(key)

    diction=get_yaml(list_yaml, key)

    print(type(diction))

    if isinstance(diction, dict):
        return STATUS_OK, yaml.dump(list(diction.keys()))
    else:
        return diction, []

METHODS={
    'KEYS':method_KEYS,
    'GET':method_GET,
    'FIELDS':method_FIELDS,
}

def handle_request(req):
    if req.method in METHODS:
        return METHODS[req.method](req.content)
    else:
        return STATUS_UNKNOWN_METHOD,[]
    
def send_response(f,status,content):

    f.write(b"S->C:" + f"{status[0]} {status[1]}\n".encode('utf-8'))
    if(len(content) > 0):
        f.write(b"S->C:" + content.encode('utf-8') + b'\n')
    # f.write(b'S->C:\n')
    f.flush()
    return

class Request:

    def __init__(self,f):
        
        lines=[]
        while True:
            line=f.readline()
            line=line.decode('utf-8')
            if line=='':
                raise ConnectionClosed
            if line=='\n':
                break
            line=line.rstrip()
            logging.debug(f'Client sent {line}')
            lines.append(line)
        if not lines:
            raise BadRequest
        self.method=lines[0]
        try:
            self.content=[line for line in lines[1:]]
        except ValueError: 
            raise BadRequest

def handle_client(client_socket,addr):

    logging.info(f'handle_client {addr} start')
    f=client_socket.makefile('rwb')

    # client_socket.sendall("C->S:".encode('utf-8'))
    # f.write("C->S:".encode('utf-8'))

    while True:
        try:
            req=Request(f)
            logging.info(f'Request: {req.method} {req.content}')
            status,response=handle_request(req)

            send_response(f,status,response)

            if status == STATUS_UNKNOWN_METHOD:
                break

        except BadRequest:
            logging.info('Bad request',addr)
            break 
        except ConnectionClosed:
            logging.info('Connection closed',addr)
    logging.info(f'handle_client {addr} stop')

s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',9999))
s.listen(5)

while True:
    
    cs,addr=s.accept()
    process=multiprocessing.Process(target=handle_client,args=(cs,addr))
    process.daemon=True
    process.start()
    cs.close()