"""https://gist.github.com/phizaz/c62162f45ca1f453ce265d09222ee96d"""

from filelock import FileLock
from pathlib import Path
import random, socket
 
class BindFreePort(object):
    def __init__(self, start, stop):
        self.port = None
 
        self.sock = socket.socket()
 
        while True:
            port = random.randint(start, stop)
            try:
                self.sock.bind(('', port))
                self.port = port
                break
            except Exception:
                continue
 
    def release(self):
        assert self.port is not None
        self.sock.close()
 
 
class FreePort(object):
    used_ports = set()
 
    def __init__(self, start=20000, stop=30000):
        self.lock = None
        self.bind = None
        self.port = None
 
        import time
        while True:
            bind = BindFreePort(start, stop)
 
            if bind.port in self.used_ports:
                bind.release()
                continue
 
            '''
            Since we cannot be certain the user will bind the port 'immediately' (actually it is not possible using
            this flow. We must ensure that the port will not be reacquired even it is not bound to anything
            '''
            Path('/tmp/sapp').mkdir(parents=True, exist_ok=True)
            lock = FileLock('/tmp/sapp/port_{}'.format(bind.port))
            success = lock.acquire(blocking=False)
 
            if success:
                self.lock = lock
                self.port = bind.port
                self.used_ports.add(bind.port)
                bind.release()
                break
 
            bind.release()
            time.sleep(0.01)
 
    def release(self):
        assert self.lock is not None
        assert self.port is not None
        self.used_ports.remove(self.port)
        self.lock.release()