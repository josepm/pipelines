__author__ = 'josep'

"""
A basic pipeline that reads avro files which are json lines and saves the data into a pandas dataframe

"""

import sys
import os
import pandas as pd
import json

# add the path to utilities
f = os.path.dirname(os.path.realpath(__file__))
par_dir = os.path.abspath(os.path.join(f, os.pardir))
sys.path.append(par_dir)

import pipeline as pp
import utilities.sys_utils as su


if __name__ == '__main__':
    args = su.get_pars([sys.argv[1]])

    #  pipeline parameters
    top_list = args['top_list']
    pattern = args['pattern']
    filepat_list = args['filepat_list']
    ex_list = args['ex_list']
    out_file = args['out_file']

    # pipeline
    pl = pp.pipeline(filepat_list, top_list, ex_list, pattern, cat_type='avro', print_file=False)
    d_out = (json.loads(d) for d in pl)
    df = pd.DataFrame(d_out)
    if len(df) == 0:
        print 'no data'
        sys.exit(0)
    su.df_to_json_lines(df, out_file)
