#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import RCFNavigator
from RCFNavigator import DateGetter


def main():
    date = DateGetter().formatted_date()
    print(date)
    return date


if __name__ == "__main__":
    main()
