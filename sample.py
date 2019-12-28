import getpass
import random
import numpy as np
import os
import timeit

import pandas as pd

if os.environ['method'] == 'trigger':
    from trigger import create_engine
elif os.environ['method'] == 'pyapi':
    from my_sqlalchemy import create_engine
else:
    pass

def gen_data(cols_type, null_ratio, data_num):
    data = [[] for _ in range(len(cols_type))]
    for i, col in enumerate(cols_type):
        if col.lower() == 'int':
            null_index = random.sample(range(data_num), int(data_num*null_ratio[i]))
            data[i] = [None if i in null_index else 1 for i in range(data_num)]
        else:
            null_index = random.sample(range(data_num), int(data_num*null_ratio[i]))
            data[i] = ['NULL' if i in null_index else 'a' for i in range(data_num)]
    print(data)
    return list(zip(*data))

def run():
    """ Run! """
    # NOTE: the database 'test' should be created before executing this script
    database = 'test'
    table_name = 'person'

    sql_password = getpass.getpass()
    create_str = (f'mysql+mysqlconnector://root:{sql_password}@localhost/'
                  f'{database}?charset=utf8mb4')
    engine = create_engine(create_str)

    # engine._print_debug_msg = True
    engine.create_table(table_name, (('ID', 'int'),
                                     ('Name', 'varchar(255)')))

    engine.insert(table_name, ('ID', 'name'), ((None, 'hi'), (1, 'NULL'), (2, 'orz')))

    print(pd.read_sql_query(f'SELECT * FROM {table_name}', engine._engine))
    ana_table_name = engine._make_ana_name(table_name, "null")
    print(pd.read_sql_query(f'SELECT * FROM {ana_table_name}', engine._engine))

    engine.drop_table(table_name)

    engine.create_table(table_name, (('ID', 'int'),
                                     ('Name', 'varchar(255)')))
    data = gen_data(['int', 'str'], [0.1, 0.2], int(1e5))
    start_time = timeit.default_timer()
    engine.insert(table_name, ('ID', 'name'), data)
    elapsed = timeit.default_timer() - start_time
    print(f"takes {elapsed} seconds to complete.")

    engine.drop_table(table_name)

if __name__ == '__main__':
    run()
