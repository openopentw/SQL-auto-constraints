import argparse
import getpass
import random
import numpy as np
import os
import timeit

import pandas as pd


def gen_data(cols_type, null_ratio, data_num):
    data = [[] for _ in range(len(cols_type))]
    
    for i, col in enumerate(cols_type):
        null_index = random.sample(range(data_num), int(data_num*null_ratio[i]))

        if col.lower() == 'int':     
            data[i] = [None if i in null_index else 1 for i in range(data_num)]
        else:
            data[i] = ['NULL' if i in null_index else 'a' for i in range(data_num)]
    
    return list(zip(*data))

def run(args):
    """ Run! """
    
    # import controlled by `method` argument
    if args.method == 'trigger':
        from trigger import create_engine
    elif args.method == 'pyapi':
        from my_sqlalchemy import create_engine
    else: pass


    # NOTE: the database 'test' should be created before executing this script
    database = 'test'
    table_name = 'person'
    mission="null"

    sql_password = getpass.getpass()
    create_str = (f'mysql+mysqlconnector://root:{sql_password}@localhost/'
                  f'{database}?charset=utf8mb4')
    engine = create_engine(create_str)

    # engine._print_debug_msg = True
    engine.create_table(table_name, (('ID', 'int'),
                                     ('Name', 'varchar(255)')))

    engine.insert(table_name, ('ID', 'Name'), ((None, 'hi'), (1, 'NULL'), (2, 'orz')))

    print(pd.read_sql_query(f'SELECT * FROM {table_name}', engine._engine))
    ana_table_name = engine._make_ana_name(table_name, mission)
    print(pd.read_sql_query(f'SELECT * FROM {ana_table_name}', engine._engine))

    engine.drop_table(table_name)

    if args.test:
        print("Start Testing...")
        engine.create_table(table_name, (('ID', 'int'),
                                        ('Name', 'varchar(255)')))
        data = gen_data(['int', 'str'], [0.1, 0.2], args.test_num)
        start_time = timeit.default_timer()
        engine.insert(table_name, ('ID', 'Name'), data)
        elapsed = timeit.default_timer() - start_time
        ana_table_name = engine._make_ana_name(table_name, mission)
        print(pd.read_sql_query(f'SELECT * FROM {ana_table_name}', engine._engine))
        print(f"takes {elapsed} seconds to complete.")

        engine.drop_table(table_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--method", 
                        type=str, 
                        default="pyapi", 
                        choices=["trigger", "pyapi"], 
                        help="Choose one method for implementation.")
    parser.add_argument("-t", "--test", 
                        action="store_true", 
                        help="Test or not")
    parser.add_argument("--test_num",
                        type=int,
                        default=10000,
                        help="Specify the number of testing samples.")

    args = parser.parse_args()
    run(args)
