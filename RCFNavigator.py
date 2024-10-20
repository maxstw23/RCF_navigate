#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import queue
import subprocess as sp
import os
import time
import smtplib
from typing import Any
import signal
import threading


class RCFNavigator:
    """
    A platform on which various functionalities related to streamlining RCF workflow can be built upon.
    Some examples include finding the node on which the current jobs related to your current working directory
    is running on, or automatically killing jobs that have been running for a certain period of time.

    This could be much more helpful if I figured out how to navigate RCF's 'rterm' terminal procedure.
    This should be doable in theory, but I am still not familiar enough with channeling displays to make it work.
    It also may be a platform-dependent thing. But if anyone figured out how to do it, please let me know.
    """
    def __init__(self, command):
        """
        Initializer for RCF Navigator
        :param command: the command associated with the RCF query (i.e., condor_q, condor_rm)
        """
        self.p = sp.Popen(command, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        self.out, self.err = self.p.communicate()

    def get_process(self):
        """
        :return: the subprocess process
        """
        return self.p

    def get_output(self):
        """
        Usually how information is obtained
        :return: the std output of the process
        """
        return self.out
    
    def get_error(self):
        """
        :return: the std error of the process
        """
        return self.err

class AutoBuffer(queue.Queue):
    """
    A simple helper structure based on queue.Queue that automatically pops out the first-in
    element when putting in new element when the buffer is full. Used to find which node
    a certain job on RCF is running
    """
    def __init__(self, maxsize=0):
        super().__init__(maxsize)

    def fill(self, element):
        """
        A special operation that should replace put().
        It pops out the first-in element when the buffer is full
        :param element: the new element to be filled
        :return: whatever put(element) returns, I think None?
        """
        if self.full():
            self.get()
        self.put(element)

class NodeChecker:
    """
    Useful tool to check which node your jobs are running.
    Currently, only support checking jobs related to current working directory.
    More utilities could be added if needed.
    """
    user = os.environ.get('USER')
    cwd = os.environ.get('PWD') + '/'
    command = f'condor_q -global {user}'

    def __init__(self):
        """
        Initializer, pretty does everything already
        """
        self.navigator = RCFNavigator(self.command)
        self.buffer = AutoBuffer(maxsize=2)
        for line in self.navigator.get_output().split(b'\n'):
            # Popen std output contains an extra new line at the end
            line_str = line[:-1].decode('utf-8')
            if self.cwd in line_str:
                # two lines before the line containing pwd
                self.node = self.buffer.get().split()[2].split('.')[0]
                break
            self.buffer.fill(line_str)

    def get_node(self):
        """
        Get the node string, e.g. rcas6006
        :return: the node string
        """
        return self.node

    def get_node_num(self):
        """
        Get the string number, e.g. 6006 (probably will never need it)
        :return: the node integer
        """
        return int(self.get_node()[4:])

class LongKiller:
    """
    A job killer that targets jobs that have been running too long. Thresholds can be set
    For instance, to kill jobs that have been running for one day, you can do
        job_killer = LongKiller(1,0)
        job_killer.kill_bad_job()
    """
    user = os.environ.get('USER')
    cwd = os.environ.get('PWD') + '/'
    node = os.environ.get('HOST')
    command = f'condor_q -global {user} | grep {cwd}'

    def __init__(self, _day, _hour=0, local=False):
        """
        Initializer, does pretty much everything
        :param _day: Day threshold
        :param _hour: Hour threshold
        """
        self.navigator = RCFNavigator(self.command)
        self.local = local
        if self.local:
            self.command = f'condor_q {self.user} | grep {self.cwd}'
        self.bad_id_list = []
        self.bad_sched_list = []
        self.hour_threshold = _day*24+_hour

        for line in self.navigator.get_output().split(b'\n'):
            if line == b'':
                break
            # Popen std output contains an extra new line at the end
            line_str = line[:-1].decode('utf-8')
            run_time_str = line_str.split()[4]
            day = int(run_time_str.split('+')[0])
            hour = day*24+int(run_time_str.split('+')[1].split(':')[0])

            if hour >= _day*24+_hour:
                self.bad_id_list.append(line_str.split()[0])
                self.bad_sched_list.append(line_str.split()[11].split('/')[-1].split('.')[0])
            else:
                break

    def bad_id(self):
        """
        Just return the bad ids
        :return: ids of jobs that have been running for longer than the threshold
        """
        return self.bad_id_list

    def kill_bad_job(self):
        """
        Actually kill the bad jobs found. Have to be on the same node though.
        It seems a user can either kill all jobs from an arbitrary node
        by doing condor_rm -name $NODE $USER, or kill a specific job from the same
        node using condor_rm $JOBID, but not a specific job from an arbitrary node!

        When running jobs from more than one directory on the same node, the node
        getter may not return the correct node. But if you are on the right node,
        you should still be able to kill the jobs corresponding to your PWD by overriding
        (answering 'y' to the question 'Kill jobs anyway?')
        """
        # verify we are on the correct node
        if not self.local:
            correct_node = NodeChecker().get_node()
            if correct_node != self.node:
                print(f'You are not on the right node! Go to {correct_node}.')
                override = input("Kill jobs anyway? (y/n)")
                if override != 'y':
                    return

        # job killer
        print(f'Killing {len(self.bad_id_list)} jobs that have been running for more than {self.hour_threshold} hours')
        for process in self.bad_id_list:
            sp.run(['condor_rm', process])

    def kill_and_resubmit(self, rel_path = '.'):
        """
        Kill the bad jobs found and resubmit if the sched file can be found
        in the relative path provided
        :param rel_path: relative path where the sched is store, default is
                         current directory
        """
        # verify we are on the correct node
        correct_node = NodeChecker().get_node()
        if correct_node != self.node:
            print(f'You are not on the right node! Go to {correct_node}.')
            override = input("Kill jobs anyway? (y/n)")
            if override != 'y':
                return

        # job killer and resubmitter
        for index, process in enumerate(self.bad_id_list):
            sched = self.bad_sched_list[index].split('_')[0]
            job_number = self.bad_sched_list[index].split('_')[1]
            # sp.run(['condor_rm', process])
            sp.run(['star-submit', '-kr', job_number, f'{sched}.session.xml'])

class DateGetter:
    """
    Find the current date, output in Month_Date format, e.g., March_26
    """
    command = 'date'

    def __init__(self):
        self.navigator = RCFNavigator(self.command)
        line = self.navigator.get_output().split(b'\n').readline()
        self.month, self.date = [line.split()[i].decode('utf-8') for i in (1, 2)]
        # print(f'{month}_{date}')

    def formatted_date(self):
        return f'{self.month}_{self.date}'

class FileMover:
    """
    Find files in the working directory with same names as those in another given directory,
    then move those files to a third designated directory
    """
    command = 'ls -1a '

    def __init__(self, query_dir):
        """
        Initialize and obtain a list of file names
        :param query_dir: the directory with files with the desired names (NOT the working directory)
        """
        self.navigator = RCFNavigator(self.command + query_dir)
        self.filenames = [filename[:-1].decode('utf-8') for filename in self.navigator.get_output().split(b'\n')][2:]

    def move(self, working_dir, target_dir):
        for file in self.filenames:
           sp.run(['mv', working_dir+file, target_dir+file])
        # print(self.filenames)

class JobMonitor:
    """
    Monitor the status of jobs on RCF. Automatically resubmit if there are many missing files.
    Email notification on completion.
    """
    user = os.environ.get('USER')
    cwd = os.environ.get('PWD') + '/'
    node = os.environ.get('HOST')
    command = f'condor_q {user} | grep {cwd}'
    command_missing = f'python check_missing_files.py'
    command_resubmit = f'sh resubmit.sh'

    def __init__(self, email, days=1, hours=0, debug=False, glob=False):
        self.email = email
        self.count_missing = 0
        self.count_all = 0
        self.days = days
        self.hours = hours
        self.debug = debug
        self.glob = glob
        if self.glob:
            self.command = f'condor_q -global {self.user} | grep {self.cwd}'

    def check_queue(self):
        ### number of jobs found
        if self.debug:
            print('Checking queue...')
        while True:
            navigator = RCFNavigator(self.command)
            count_all = 0
            count_running = 0
            count_idle = 0
            count_held = 0
            status = 0
            if self.debug:
                print('Checking command error...')
            if self.glob:
                self.node = NodeChecker().get_node()
            for line in navigator.get_error().split(b'\n'):
                if 'Failed to fetch ads' in line.decode('utf-8') and self.node in line.decode('utf-8'):
                    print('Node is unaccessible, recheck in 10 minutes')
                    status = 1
                    break
            if status == 1:
                time.sleep(600)
                continue
            if self.debug:
                print('Checking command output...')
            for line in navigator.get_output().split(b'\n'):
                if self.user not in line.decode('utf-8') or ' X ' in line.decode('utf-8'):
                    continue
                count_all += 1
                if ' R ' in line.decode('utf-8'):
                    count_running += 1
                if ' I ' in line.decode('utf-8'):
                    count_idle += 1
                if ' H ' in line.decode('utf-8'):
                    count_held += 1
            break

        self.count_all = count_all
        print(f'Jobs found: {count_all}, Running: {count_running}, Idle: {count_idle}, Held: {count_held}')

        # releasing held jobs
        if count_held > 0:
            print('Releasing held jobs...')
            navigator = RCFNavigator(f'condor_release {self.user}')
            for line in navigator.get_output().split(b'\n'):
                print(line.decode('utf-8'))
    
    def check_missing(self):
        navigator = RCFNavigator(self.command_missing)
        self.count_missing = int(navigator.get_output().split(b'\n')[0].decode('utf-8'))
        print(f'Missing files: {self.count_missing}')

    def resubmit(self):
        navigator = RCFNavigator(self.command_resubmit)
        output = navigator.get_output().split(b'\n')
        resubmit_count = 0
        for line in output:
            if 'files for process' in line.decode('utf-8') and 'done' in line.decode('utf-8'):
                resubmit_count += 1
        if resubmit_count != self.count_missing:
            print(f'WARNING: number of resubmission ({resubmit_count}) does not match number of missing files ({self.count_missing}). Killing all jobs...')
            LongKiller(0, 0, local=True).kill_bad_job()
        print(f'{self.count_missing} jobs resubmitted')
    
    def email_notification(self):
        # just send email using shell mail (blank email body)
        script = '''{{
    echo To: {0}
    echo Subject: Job completion notification
    echo
    echo Jobs on node {1} have completed. Please check the output files in {2}.
}} | /usr/sbin/sendmail -t'''.format(self.email, self.node, self.cwd)
        sp.run(script, shell=True)
               
    def task(self):
        self.check_queue()
        LongKiller(self.days, self.hours, local=True).kill_bad_job()
        self.check_missing()
        if self.count_missing < 5:
            if self.count_all == 0:
                self.email_notification()
                return True
        elif self.count_all == 0:
            print('No jobs found, resubmitting...')
            self.resubmit()
        elif self.count_missing > 20 * self.count_all:
            # it might be worth it to kill all jobs and resubmit in this case
            print('Too many missing files, kill and resubmit remaining jobs')
            LongKiller(0, 0, local=True).kill_bad_job()
            self.resubmit()
        return False

    def loop(self, exit: threading.Event):
        print(f'Starting from node {self.node}...')
        while True:
            done = self.task()
            if done:
                break
            print('Check again in 10 minutes. Press Ctrl+\\ to check now')
            exit.wait(600)
            if exit.is_set():
                exit.clear()
                continue
    
    def start(self):
        exit = threading.Event()
        def quit(signo, _frame):
            print(f'Interrupted by {signo}, checking now')
            exit.set()
        signal.signal(signal.SIGQUIT, quit)
        self.loop(exit)