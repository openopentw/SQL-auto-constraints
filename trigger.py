""" Base class for my sqlalchemy. """

from sqlalchemy import create_engine as their_create_engine

def create_engine(create_str):
    """ My create engine. """
    return MySqlAlchemy(create_str)

class MySqlAlchemy:
    """ Base class for my sqlalchemy. """
    def __init__(self, create_str):
        self._engine = their_create_engine(create_str)
        self._print_debug_msg = False
        self._constraints = ['null', 'default', 'unique']

    # transfer strings

    def _make_ana_name(self, table_name, constrain): # pylint: disable=no-self-use
        """ Convert normal table_name to analysis table_name. """
        return f'_{table_name}_{constrain}_analysis'

    def __value_to_str_1d(self, values): # pylint: disable=no-self-use
        """
        Args:
            values: (List/Tuple) [str/int]
        """
        # value_str = [str(elm) for elm in values]
        value_str = []
        for elm in values:
            if elm is None:
                value_str.append("NULL")
            elif isinstance(elm, int):
                value_str.append(str(elm))
            elif isinstance(elm, str):
                value_str.append('"' + elm + '"' if elm != "NULL" else elm)
            else:
                raise AssertionError(f'elm type error: {type(elm)}')
        return '(' + ','.join(value_str) + ')'

    def __value_to_str_2d(self, values):
        """
        Args:
            values: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        value_row = [self.__value_to_str_1d(row) for row in values]
        print(','.join(value_row))
        return ','.join(value_row)

    def _value_to_str(self, values):
        """
        Args:
            values: (List/Tuple) [str/int]
                    or (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        if isinstance(values[0], (list, tuple)):
            return self.__value_to_str_2d(values)
        return self.__value_to_str_1d(values)

    # execute sql commands

    def _execute(self, *args, **kwargs):
        """ Execute sql commands. """
        if self._print_debug_msg:
            print(args, kwargs)
        return self._engine.execute(*args, **kwargs)

    def execute(self, *args, **kwargs):
        """ Execute sql commands and check not insert before executing. """
        assert ('insert' not in args[0].lower()), 'Please use insert function instead!'
        return self._execute(*args, **kwargs)

    def _get_columns(self, table_name):
        """ get columns of the table"""
        return [
            col.Field
            for col in self._execute(f"SHOW columns FROM {table_name}")
        ]

    def _create_null_trigger(self, table_name, ana_table_name):
        """ create not null trigger when user creating table"""
        columns = self._get_columns(table_name)
        col_trigger_str = [
            f"""
                SET null_count = (SELECT null_cnt FROM {ana_table_name} WHERE col_name = '{col}');
                SET total_count = (SELECT total_cnt FROM {ana_table_name} WHERE col_name = '{col}');

                IF NEW.{col} IS NULL THEN
                    UPDATE {ana_table_name} SET null_cnt = null_cnt + 1 WHERE col_name = '{col}';
                END IF;
                UPDATE {ana_table_name} SET total_cnt = total_cnt + 1 WHERE col_name = '{col}';
            """
            for col in columns
        ]

        trigger = f"""
                CREATE TRIGGER null_{table_name}_TRIGGER 
                BEFORE INSERT ON {table_name}

                FOR EACH ROW 
                BEGIN
                    DECLARE null_count INTEGER;
                    DECLARE total_count INTEGER;
                """ + "\n".join(col_trigger_str) + "END;"
                
        return self._execute(trigger)

    def _create_default_trigger(self, table_name, ana_table_name):
        pass

    def _create_unique_trigger(self, table_name, ana_table_name):
        pass

    # modify tables
    
    def _create_table(self, table_name, cols):
        """ Create table.
        Args:
            table_name: str
            cols: (List/Tuple) [str] w. shape (D, 2)
                e.g.: [['ID', 'int'],
                       ['Name', 'varchar(255)']]
        """
        type_str = ','.join([' '.join(col) for col in cols])
        sql_str = (f'CREATE TABLE {table_name} ({type_str})')
        self._execute(sql_str)

    def _drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._execute(f'DROP TABLE {table_name}')

    def create_table(self, table_name, cols):
        """ Create table and other essential tables.
        Args:
            table_name: str
            cols: (List/Tuple) [str] w. shape (D, 2)
                e.g.: [['ID', 'int'],
                       ['Name', 'varchar(255)']]
        """
        self._create_table(table_name, cols)

        for cons in self._constraints:
            ana_table_name = self._make_ana_name(table_name, cons)
            self._create_table(ana_table_name, (('col_name', 'varchar(255)'),
                                            (f'{cons}_cnt', 'int'),
                                            ('total_cnt', 'int')))
            ana_cols = []
            for col in cols:
                ana_cols.append([col[0], 0, 0])
            self._insert(ana_table_name, ('col_name', f'{cons}_cnt', 'total_cnt'), ana_cols)
            eval(f'self._create_{cons}_trigger(table_name, ana_table_name)')


    def drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._drop_table(table_name)
        
        for cons in self._constraints:
            self._drop_table(self._make_ana_name(table_name, cons))

    # modify rows

    def _insert(self, table_name, cols, vals):
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        sql_str = (f'INSERT INTO {table_name}'
                   f' ({", ".join(cols)})'
                   f' VALUES {self._value_to_str(vals)}')
        self._execute(sql_str)

    def _update(self, table_name, cols, cond=''):
        """ Update rows in a table.
        Args:
            cols: (List/Tuple) [ (List/Tuple) [str/int] ]
            cond: str
        """
        cols_str = ','.join(['='.join(col) for col in cols])
        sql_str = (f'UPDATE {table_name}'
                   f' SET {cols_str}')
        if cond:
            sql_str += f' WHERE {cond}'
        self._execute(sql_str)

    def _insert_ana_null(self, ana_table_name, cols, vals):
        """ Check Not Null constraint. """
        # cols not in col_name
        in_col = ','.join(['"' + col + '"' for col in cols])
        self._update(ana_table_name,
                     [['null_cnt', f'null_cnt+{len(vals)}']],
                     f'col_name NOT IN ({in_col})')

        # count "NULL"s
        null_cnt = [0] * len(cols[0])
        for row in vals:
            for i, val in enumerate(row):
                if str(val).lower() == 'null':
                    null_cnt[i] += 1
        for i, col in enumerate(cols):
            if null_cnt[i] > 0:
                self._update(ana_table_name,
                             [['null_cnt', f'null_cnt+{null_cnt[i]}']],
                             f'col_name = "{col}"')
    def _insert_ana_default(self, ana_table_name, cols, vals):
        ### TO-DO
        pass
    
    def _insert_ana_unique(self, ana_table_name, cols, vals):
        ## TO-DO
        pass

    def insert(self, table_name, cols, vals):
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        self._insert(table_name, cols, vals)

        #for cons in self._constraints:
        #    ana_table_name = self._make_ana_name(table_name, cons)
            #eval(f'self._insert_ana_{cons}(ana_table_name, cols, vals)')

    def delete(self, table_name, cond):
        """ Delete some rows. """
        # TODO: update analysis informations

