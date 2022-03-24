import sys
import os
import time
from fabric import Connection
from invoke import Responder
from pynput.keyboard import Key, Controller


class RCFInterface:
    host = os.environ.get('RCFSSH')
    username = os.environ.get('RCFUSER')
    key_dir = os.environ.get('RCFKEYDIR')
    password = os.environ.get('RCFPWD')
    node = "6015"

    def __init__(self):
        self.connection = Connection(self.host,
                                     user=self.username)
        # connect_kwargs={"key_filename": self.key_dir})

    def __del__(self):
        self.connection.close()

    def login(self, _node):
        password = Responder(pattern='{}@rcas{}\'s password'.format(self.username, _node),
                             response='{}\n'.format(self.password), )
        self.connection.run("rterm -i rcas{}".format(_node), pty=True, watchers=[password])
        # self.connection = self.connection.create_session()


def main():
    node = "6015"
    mysession = RCFInterface()
    mysession.login(node)
    time.sleep(5)

    command = 'cd /star/data01/pwg/xiatong/git\n'
    keyboard = Controller()
    keyboard.type(command)


if __name__ == '__main__':
    main()
