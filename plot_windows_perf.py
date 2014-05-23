#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python(全部入り)
 - WinPython2.7 http://sourceforge.net/projects/winpython/files/WinPython_2.7/2.7.6.3/
参考資料
 - http://pandas.pydata.org/pandas-docs/stable/10min.html
引数
 CSVファイル
出力
 distフォルダ以下に png ファイルを出力
"""
import sys
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
from matplotlib import font_manager, dates
from matplotlib.ticker import ScalarFormatter

########## パラメータ 開始 ##########
# CPUコア数
CPU_CORE_NUM = 4
# pngファイル出力ディレクトリ
DEFAULT_OUTPUT_DIR = 'dist'
# '%m/%d %H:%M' - 12/31 11:59
# '%H:%M:%S' - 11:59:59
X_AXIS_FORMATTER = dates.DateFormatter('%H:%M:%S')
# MinuteLocator(interval=10) - 10分おき
# HourLocator() - 1時間おき
# DayLocator() - 1日おき
X_AXIS_LOCATOR = dates.MinuteLocator(interval=10)
########## パラメータ 終了 ##########

def is_byte_column(column):
    if (re.search(r'.* Bytes.*', column) and re.search(r'.*% .*', column) == None ):
        return True
    elif (re.search(r'.*Working Set.*', column)):
        return True
    elif (re.search(r'.*Commit Limit.*', column)):
        return True
    else:
        return False


for file in sys.argv[1:]:
    df = pd.read_csv(file, index_col=0, low_memory=False, na_filter=False)
    df.index = df.index.to_datetime()
    for column in df.columns:
        _fig = plt.figure(figsize=(12, 4))
        try:
            # init) start
            # init) y top and y label
            _ytop, _ylabel, _title = None, None, str(column)
            if (is_byte_column(str(column))):
                df[column] = df[column] / (1024.0 * 1024.0)
                _title = str(column).replace(' Bytes', ' MBytes')
                _ylabel = 'MBytes'

            # init) calculate min/mean/max
            _min = df[column].min()
            _mean = df[column].mean()
            _max = df[column].max()

            if (re.search(r'.*% .*', str(column))):
                _ylabel = '%'
                if (re.search(r'.*Process\(.*', str(column))):
                    _ytop = CPU_CORE_NUM * 100 + 5
                else:
                    if (_max < 105):
                        _ytop = 105
            elif (re.search(r'.*?[a-zA-Z0-9]+/[a-zA-Z0-9]+$', str(column))):
                _ylabel = re.sub(r'.*?([a-zA-Z0-9]+)/([a-zA-Z0-9]+)$',
                                 r'\1 Per \2',
                                 str(column)).replace('Bytes Per', 'MBytes Per').capitalize()

            # create graph) start
            _ax1 = _fig.add_subplot(111)
            _tmp_label = str(column).replace('\\\\', '')
            _line = _ax1.plot(df.index,
                              df[column].tolist(),
                              alpha=0.75,
                              label=_tmp_label)
            _fig.subplots_adjust(top=0.9, bottom=0.15, right=0.90, left=0.10)
            _ax1.grid(True)
            _ax1.yaxis.grid(True, linestyle='-', which='major', color='grey')
            _ax1.yaxis.grid(True, linestyle='-', which='minor', color='lightgrey')
            _ax1.yaxis.set_major_formatter(ScalarFormatter(False))
            plt.title(_title)
            _prop = font_manager.FontProperties(size='small')

            # create graph) set x axis and tick
            plt.gca().xaxis.set_major_formatter(X_AXIS_FORMATTER)
            plt.gca().xaxis.set_major_locator(X_AXIS_LOCATOR)
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.grid(True, which='minor')
            plt.tick_params(axis='both', which='major', labelsize=10)

            # create graph) set y label and lim
            if (_ytop):
                _ax1.set_ylim(bottom=0, top=_ytop)
            else:
                _ax1.set_ylim(0, )
            if (_ylabel):
                _ax1.set_ylabel(_ylabel)

            # create graph) set legend
            plt.figlegend((_line), (('min: ' + str(_min) + ", avg: " + str(_mean) + ", max: " + str(_max) ), ),
                          'lower center', prop=_prop, ncol=3)

            print "\t".join([column, _title, str(_ylabel), str(_min), str(_max), str(df[column].dtype)])

            # create graph) save png
            _column_str = str(column).replace('\\\\', '').replace('\\', '_')
            plt.savefig(DEFAULT_OUTPUT_DIR + '/' + "".join(
                [c for c in _column_str
                 if c.isalpha() or c.isdigit() or c == '_' or c == ' ' or c == '(' or c == ')']).rstrip()
            )
            _fig.clear()
            plt.close()
        except:
            # skip error
            print "skip:", str(column)
            _fig.clear()
            plt.close()
