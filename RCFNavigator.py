#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import queue
import subprocess as sp
import os
import time


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
        self.p = sp.Popen(command, shell=True, stdout=sp.PIPE)

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
        return self.p.stdout


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
    cwd = os.environ.get('PWD')
    command = f'condor_q -global {user}'
    navigator = RCFNavigator(command)
    buffer = AutoBuffer(maxsize=2)

    def __init__(self):
        """
        Initializer, pretty does everything already
        """
        for line in self.navigator.get_output():
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
    """
    user = os.environ.get('USER')
    cwd = os.environ.get('PWD')
    node = os.environ.get('HOST')
    command = f'condor_q -global {user} | grep {cwd}'
    navigator = RCFNavigator(command)
    bad_id_list = []

    def __init__(self, _day, _hour=0):
        """
        Initializer, does pretty much everything
        :param _day: Day threshold
        :param _hour: Hour threshold
        """
        for line in self.navigator.get_output():
            # Popen std output contains an extra new line at the end
            line_str = line[:-1].decode('utf-8')
            run_time_str = line_str.split()[4]
            day = int(run_time_str.split('+')[0])
            hour = day*24+int(run_time_str.split('+')[1].split(':')[0])
            if hour > _day*24+_hour:
                self.bad_id_list.append(line_str.split()[0])
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
        """
        # verify we are on the correct node
        correct_node = NodeChecker().get_node()
        if correct_node != self.node:
            print(f'You are not on the right node! Go to {correct_node}.')
            return

        # job killer
        for job in self.bad_id_list:
            sp.run(['condor_rm', job])

