import getpass

import pandas as pd

from my_sqlalchemy import create_engine

def run():
    """ Run! """
    # NOTE: the database 'test' should be created before executing this script
    database = 'test'
    table_name = 'person'

    sql_password = getpass.getpass()
    create_str = (f'mysql+mysqldb://root:{sql_password}@localhost/'
                  f'{database}?charset=utf8mb4')
    engine = create_engine(create_str)

    # engine._print_debug_msg = True
    engine.create_table(table_name, (('ID', 'int'),
                                     ('Name', 'varchar(255)')))

    engine.insert(table_name, ('ID', 'name'), ((0, 'hi'), (1, 'NULL')))

    print(pd.read_sql_query(f'SELECT * FROM {table_name}', engine._engine))
    ana_table_name = engine._make_ana_name(table_name)
    print(pd.read_sql_query(f'SELECT * FROM {ana_table_name}', engine._engine))

    engine.drop_table(table_name)

if __name__ == '__main__':
    run()
