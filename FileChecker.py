import tkinter as tk
from tkinter import filedialog
import pysftp
import os


def main():
    FileChecker()


class FileChecker:
    # const
    dirs = ["/star/data01/pwg/xiatong/git/Ampt_Runner/14GeV_sm/output",
            "/star/data01/pwg/xiatong/AMPT/14GeV_default",
            "/star/data01/pwg/xiatong/git/UrQMD/14GeV/"]
    local_dir = "/Users/maxwoo/Documents/GitHub/RCF_navigate/kill_long.py"
    remote_dir = "/star/data01/pwg/xiatong/git/Ampt_Runner/util_module/kill_long.py"
    host = os.environ.get('RCFSFTP')
    username = os.environ.get('RCFUSER')
    key_dir = os.environ.get('RCFKEYDIR')
    key_pass = os.environ.get('RCFKEYPASS')

    # for GUI
    dim = '720x360'
    path_width = 50
    num_width = 10

    def __init__(self):
        # set up connection
        self.sftp = pysftp.Connection(self.host, username=self.username,
                                      private_key=self.key_dir, private_key_pass=self.key_pass)

        # GUI window
        self.window = tk.Tk()
        self.window.title("RCF File Counter")
        self.window.geometry(self.dim)

        # set up file number checking
        self.dir_vars = []
        self.num_vars = []
        self.bool_vars = []
        self.set_var()
        self.check_module()

        # set up get/put
        self.rdir_var = tk.StringVar()
        self.rdir_var.set(self.remote_dir)
        self.ldir_var = tk.StringVar()
        self.ldir_var.set(self.local_dir)
        self.move_status = tk.StringVar()
        self.move_module(len(self.dirs))

        self.window.mainloop()

    def __del__(self):
        self.sftp.close()

    def set_var(self):
        # for file number checking
        for path in self.dirs:
            dir_var = tk.StringVar()
            dir_var.set(path)
            self.dir_vars.append(dir_var)

            num_var = tk.StringVar()
            num_var.set(str(0))
            self.num_vars.append(num_var)

            bool_var = tk.IntVar()
            bool_var.set(0)
            self.bool_vars.append(bool_var)

    def get_numfile(self):
        for i in range(len(self.dir_vars)):
            if self.bool_vars[i].get():
                self.num_vars[i].set('Checking')
                self.window.update()
                with self.sftp.cd(self.dir_vars[i].get()):
                    self.num_vars[i].set(str(len(self.sftp.listdir())))
                self.window.update()
            else:
                continue

        # if you want auto update, uncomment below
        self.window.after(20000, self.get_numfile)

    def get_file(self):
        self.move_status.set('Moving ...')
        self.window.update()

        # remote dir has to be a file, will implement get_d or get_r in future if needed
        if os.path.isdir(self.ldir_var.get()):
            filename = find_filename(self.rdir_var.get())
            self.sftp.get(self.rdir_var.get(), self.ldir_var.get() + filename)
        else:
            self.sftp.get(self.rdir_var.get(), self.ldir_var.get())

        self.move_status.set('Done!')
        self.window.update()

    def put_file(self):
        self.move_status.set('Moving ...')
        self.window.update()

        with self.sftp.cd(pure_path(self.rdir_var.get())):
            self.sftp.put(self.ldir_var.get())

        self.move_status.set('Done!')
        self.window.update()

    def browse(self):
        # Allow user to select a directory and store it in global var
        # called folder_path
        dirname = filedialog.askdirectory()
        self.ldir_var.set(dirname)

    def check_module(self):
        for i in range(len(self.dirs)):
            # reading directory
            tk.Label(self.window, text="Enter RCF Path: ").grid(column=0, row=i * 4 + 0)
            tk.Entry(self.window, textvariable=self.dir_vars[i], width=self.path_width).grid(column=1, row=i * 4)

            # wants to monitor?
            tk.Checkbutton(self.window, variable=self.bool_vars[i]).grid(column=3, row=i * 4)
            # showing number of files
            tk.Label(self.window, text="Current # of files: ").grid(column=0, row=i * 4 + 1)
            tk.Entry(self.window, textvariable=self.num_vars[i], width=self.num_width).grid(column=1, row=i * 4 + 1)

        # checking number of files
        tk.Button(self.window, text="Check", command=self.get_numfile).grid(column=2, row=i * 4)

    def move_module(self, index):
        tk.Label(self.window, text="Local").grid(column=0, row=index * 4 + 0)
        tk.Button(self.window, text="Browse", command=self.browse).grid(column=2, row=index * 4 + 0)
        tk.Entry(self.window, textvariable=self.ldir_var, width=self.path_width).grid(column=1, row=index * 4 + 0)
        tk.Label(self.window, text="Remote").grid(column=0, row=index * 4 + 1)
        tk.Entry(self.window, textvariable=self.rdir_var, width=self.path_width).grid(column=1, row=index * 4 + 1)

        tk.Button(self.window, text="get", command=self.get_file).grid(columnspan=2, rowspan=2)
        tk.Button(self.window, text="put", command=self.put_file).grid(columnspan=2, rowspan=2)
        tk.Entry(self.window, textvariable=self.move_status, width=self.num_width).grid(column=2, row=index * 4 + 1)


def pure_path(full_path):
    return full_path[:full_path.rfind('/')]


def find_filename(full_path):
    return full_path[full_path.rfind('/') + 1:]


if __name__ == "__main__":
    main()
