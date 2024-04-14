#!/usr/bin/env python3

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

logging.basicConfig(level=logging.DEBUG)

class BadRequest(Exception):

    pass

class ConnectionClosed(Exception):

    pass

def get_yaml(list_yaml, key):

    for item_yaml in list_yaml:
        if key in item_yaml:
            try:
                with open(f'data/{item_yaml}') as f:
                    return yaml.safe_load(f)
            except FileNotFoundError:
                return STATUS_READ_ERROR
            except OSError:
                return STATUS_READ_ERROR
            except yaml.error.YAMLError:
                return STATUS_FILE_FORMAT_ERROR
    return STATUS_NO_SUCH_KEY

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

    result = [f"{item.split('.')[0]}" for item in list_yaml]
    return STATUS_OK, yaml.dump(result)

def checks(content, method):

    chars_to_avoid = {" ", ":", "/"}

    def is_clear(chars, string):

        return all(char not in chars for char in string)

    def get_yaml_data():

        list_yaml = get_yaml_list()
        if list_yaml == STATUS_READ_ERROR:
            return STATUS_READ_ERROR, []
        return list_yaml

    if method == 'GET':
        if len(content) != 2:
            return STATUS_BAD_REQUEST, []

        key, field = [item.split(":", 1)[1] for item in content]

        if not all(is_clear(chars_to_avoid, item) for item in [key, field]):
            return STATUS_BAD_REQUEST, []

        list_yaml = get_yaml_data()
        diction = get_yaml(list_yaml, key)

        if isinstance(diction, dict):
            return (STATUS_OK, yaml.dump(diction.get(field, []))) if field in diction else (STATUS_NO_SUCH_FIELD, [])
        else:
            return diction, []

    elif method == 'FIELDS':
        if len(content) != 1 or not content[0].startswith("Key:"):
            return STATUS_BAD_REQUEST, []

        key = content[0].split(":", 1)[1]

        if not is_clear(chars_to_avoid, key):
            return STATUS_BAD_REQUEST, []

        list_yaml = get_yaml_data()
        diction = get_yaml(list_yaml, key)

        if isinstance(diction, dict):
            return STATUS_OK, yaml.dump(list(diction.keys()))
        else:
            return diction, []

def method_GET(content):

    return checks(content, 'GET')


def method_FIELDS(content):

    return checks(content, 'FIELDS')

METHODS={
    'KEYS':method_KEYS,
    'GET':method_GET,
    'FIELDS':method_FIELDS,
}

def handle_request(req):

    if req.method in METHODS:
        return METHODS[req.method](req.content)
    else:
        return STATUS_UNKNOWN_METHOD, []

def send_response(f, status, content):
    
    logging.info(f'Server sent:({status}\n{content})')
    f.write(f"{status[0]} {status[1]}\n".encode('utf-8'))
    if status == STATUS_OK:
        f.write(f'Content-length:{len(content)}\n\n'.encode('utf-8'))
        f.write(f"\n".encode('utf-8'))
        f.write(content.encode('utf-8'))
    f.write(f"\n".encode('utf-8'))
    f.flush()

class Request:

    def __init__(self, f):

        lines = []
        while True:
            line = f.readline()

            if not line:
                raise ConnectionClosed
            
            line = line.decode('utf-8').rstrip()
            logging.debug(f'Client sent {line}')

            if line == '' or line == '\n':
                break
            lines.append(line)
        if not lines:
            raise BadRequest
        
        self.method = lines[0]
        self.content=lines[1:]

def handle_client(client_socket, addr):

    f = client_socket.makefile('rwb')
    logging.info(f'handle_client {addr} start')

    try:
        while True:
            req = Request(f)
            logging.info(f'Request: {req.method} {req.content}')

            status,response=handle_request(req)

            send_response(f, status, response)

    except BadRequest as e:
        logging.info(f'Bad request {addr}')
    except ConnectionClosed as e:
        logging.info(f'Conection closed {addr}')
    except ConnectionResetError as e:
        logging.error(f'{e} {addr}\n')

    logging.info(f'handle_client {addr} stop')
    
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',9999))
s.listen(5)

while True:

    try:
        cs, addr = s.accept()
        process = multiprocessing.Process(target=handle_client, args=(cs, addr))
        process.daemon = True
        process.start()
        cs.close()
    except KeyboardInterrupt:
        logging.error('Keyboard interrupt, closing server...')
        s.close()
        exit()
