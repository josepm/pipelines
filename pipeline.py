__author__ = 'josep'
"""
generic Python pipeline functions
Extends David Beazley's examples
A set of pipelining utilities
"""

import gzip
import bz2
import fastavro
import fnmatch
import os
from datetime import timedelta
from datetime import datetime
import re
import sys
import json
import glob


def gen_open(filenames):
    """
    opens a file for reading
    :param filenames: file path
    :return:
    """
    for name in filenames:
        if name.endswith('.gz'):
            yield gzip.open(name)
        elif name.endswith('.bz2'):
            yield bz2.BZ2File(name)
        else:
            yield open(name)


def gen_cat(sources, cat_type=None):
    for s in sources:
        if cat_type is None:
            for item in s:
                yield item
        elif cat_type == 'avro':
            reader = fastavro.reader(s)
            for item in reader:
                yield json.dumps(item)
        else:
            print 'unknown cat type: ' + str(cat_type)


def gen_split(pat, lines, parts):
    """
    return the items in the split of each line in line by pat
    :param pat: pattern (regex)  r'somepattern'
    :param lines: list of lines
    """
    if pat is not None:
        patc = re.compile(pat)
        for line in lines:
            vline = patc.split(line)
            if len(vline) == parts:
                yield vline
    else:
        for line in lines:
            yield [line]


def gen_grep(lines, pat_list=[]):
    """
    grep lines that match pattern
    :param pat: a regex list or an empty list,  [r'somepattern', ...]
    :param lines: a string
    """
    for line in lines:
        if len(pat_list) > 0:
            yield (line for pat in pat_list if re.compile(pat).search(line))
            # for pat in pat_list:
            #     patc = re.compile(pat)
            #     if patc.search(line):
            #         yield line
        else:
            yield line


def build_ex_list(start_dt, end_dt, top, ex_list, date_fmt):
    """
    We may not want to process all files in a directory.
    Often files are organized in date/time hierarchies.
    This function builds exclusion lists for gen_find()
    :param start_dt: datetime(yy, mm, dd)
    :param end_dt: datetime(yy, mm, dd)
    :param top: top directory, not a list of top directories
    :param ex_list: list to fill with exclusion dirs of the form top/...
    :param date_fmt: string to indicate the date split format: Not all formats are included.
           - '%d' for ../yy-mm/dd/<files>
           - '%m-%d'  ../yy/mm-dd/<files>
    :return: fills ex_list with the files to exclude from the pipeline
    """
    day = start_dt
    v = top.split('/')
    low_dir = v[-2]   # of the form yy-mm or yy. Assume top ends with /
    if date_fmt == '%d':
        other_fmt = '%Y-%m'
    elif date_fmt == '%m-%d':
        other_fmt = '%Y'
    else:
        print 'invalid date format'
        sys.exit(0)
    while day <= end_dt:
        y = day.strftime(other_fmt)
        if y in low_dir:
            x = day.strftime(date_fmt)
            ex_list.append(top + x)
        day += timedelta(days=1)


def get_top_list(top_path, start_date, end_date, date_fmt=None):
    """
    # top_path = top level of file search
    # date_fmt = defines the time directory hierarchy if applicable. See build_ex_list
    """
    start_dt = set_datetime(start_date)
    end_dt = set_datetime(end_date)
    top_list = []
    now_dt = start_dt
    while now_dt <= end_dt:
        if date_fmt == '%Y-%m':
            d = now_dt.strftime('%Y-%m') + '/' + now_dt.strftime('%d')
        elif date_fmt == '%Y':
            d = now_dt.strftime('%Y') + '/' + now_dt.strftime('%m-%d')
        elif date_fmt == '%Y-%m-%d':
            d = now_dt.strftime('%Y-%m-%d')
        else:
            print 'No date format match'
            d = ''
        top_list.append(top_path + d)
        now_dt += timedelta(days=1)
    top_list = list(set(top_list))
    return top_list


def get_file_names(prefix, suffix, start_date, end_date, date_fmt='%Y-%m-%d'):
    """
    Sometimes the data hierarchy is simpler.
    This function generates a list of file names of the form prefix<date>suffix to be processed
    """
    f_list = []
    start_dt = set_datetime(start_date)
    end_dt = set_datetime(end_date)
    now_dt = start_dt
    while now_dt <= end_dt:
        date = now_dt.strftime(date_fmt)
        f_list.append(prefix + date + suffix)
        now_dt += timedelta(days=1)
    return f_list


def get_ex_dates(start_date, end_date, top_list, date_fmt, min_date=None, max_date=None):
    """
    Build the list of files to stream
    :param start_date:
    :param end_date:
    :param min_date: ensures there is enough data
    :param max_date: ensures there is enough data
    :param top_list: list of top level directories
    :param date_fmt:  date_fmt: %d, %m-%d for the hierarchy
    :return:
    """
    dt_start = set_datetime(start_date)
    dt_start -= timedelta(days=1)
    dt_end = set_datetime(end_date)
    dt_end += timedelta(days=1)
    dt_min = dt_start if min_date is None else set_datetime(min_date)
    dt_max = dt_end if max_date is None else set_datetime(max_date)
    ex_list = []
    for t in top_list:
        build_ex_list(dt_min, dt_start, t, ex_list, date_fmt)
        build_ex_list(dt_end, dt_max, t, ex_list, date_fmt)
    return ex_list


def set_datetime(str_date):
    y, m, d = str_date.split('-')
    return datetime(int(y), int(m), int(d))


def gen_find(filepat_list, top_list, ex_dir, print_file):
    """
    generates the list of files to process
    Uses the os.walk(top) generator which returns at each call a list where each element has 3 components:
    - subdir: a dir in top (or top itself)
    - dir list: list of dirs inside subdir
    - file list: list of files in subdir
    :param filepat: list of file patterns to match
    :param top: list of top level directories
    :param ex_dir: list of directories to exclude. Elements must be of the form top/some_dir
    :return: file path of the form top/some_dir/file_name with file_name matching filepat
    """
    ctr = 0
    for d in top_list:
        d_list = glob.glob(d)
        for t in d_list:
            for path, dirlist, filelist in os.walk(t):
                if path not in ex_dir:
                    for pat in filepat_list:
                        for name in fnmatch.filter(filelist, pat):
                            ctr += 1
                            if print_file:
                                print str(ctr) + ' file: ' + str(os.path.join(path, name))
                            yield os.path.join(path, name)


def pipeline(filepat_list, top_list, ex_list, re_match_list, cat_type=None, print_file=True):
    """
    This function is called to run the basic pipeline that cleans and processes each line
    :param filepat_list: list of file patterns to match
    :param top: top level directory to iterate on
    :param ex_list: exclusion list
    :param re_match: regex to match
    :param cat_type: 'avro' or None
    :param print_file: print file name as its starts being processed
    :return: processed lines
    """
    file_names = gen_find(filepat_list, top_list, ex_list, print_file)
    files = gen_open(file_names)
    lines = gen_cat(files, cat_type)
    matched_lines = gen_grep(lines, re_match_list)
    ctr = 0
    for l in matched_lines:
        ctr += 1
        if ctr % 100000 == 0:
            print 'lines processed: ' + str(ctr)
        yield l.strip()


def to_json(line_list):
    """
    converts a line into a json dict
    :param line_list: list of lines
    :return: a dict per line
    """
    for l in line_list:
        try:
            yield json.loads(l)
        except ValueError:
            print 'cannot json load: ' + str(l)


def key_filters(dict_list, key_dict, filter_mode):
    """
    filters a dict on multiple keys.
    :param dict_list: list of json dicts
    :param key_dict: key_dict[key] = SET of allowed values for key.
           key_dict is an ordered dict so that we filter in the desired order
    :param filter_mode: all or any
    :return:
    """
    for d in dict_list:
        if filter_mode(d.get(k, None) in vals for k, vals in key_dict.iteritems()):
            yield d


def json_pipeline(js_func, in_filepat_list, in_top_list, ex_list, re_match_list, f_args=[], f_out=None, cat_type=None, print_file=True):
    """
    This pipeline converts each each input file line to a dict through a json.loads.
    After that a generator transformation js_func is allowed on each dict.
    The output of js_func is saved into f_out is not None. Otherwise, it yields the output of js_func
    Side effects on the js_func arguments (eg filling dicts, lists) can also be implemented.
    :param js_func: takes as input the list of json dicts.
    :param f_args: list of js_func args.
    :param in_filepat_list: see pipeline
    :param in_top_list: see pipeline
    :param ex_list: see pipeline
    :param re_match_list: see pipeline
    :param f_out: output file where the processed data lands
    :param cat_type: see pipeline
    :param print_file: see pipeline
    :return:
    """

    pl = pipeline(in_filepat_list, in_top_list, ex_list, re_match_list, cat_type, print_file)
    js = to_json(pl)
    if js_func:
        x_js = js_func(js, *f_args) if len(f_args) > 0 else js_func(js)
    else:
        x_js = (x for x in js)
    if f_out:
        s_out = (json.dumps(d) + '\n' for d in x_js)
        fp = gzip.open(f_out, 'wb') if f_out[-3:] == '.gz' else open(f_out, 'wb')
        fp.writelines(s_out)
        fp.close()
    else:
        for d in x_js:
            yield d


