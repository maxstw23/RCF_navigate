#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import RCFNavigator
from RCFNavigator import LongKiller
import os


def main():
    killer = LongKiller(1)
    killer.kill_bad_job()


if __name__ == "__main__":
    main()