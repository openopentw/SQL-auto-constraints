from sqlalchemy import create_engine as their_create_engine
from abc import ABC
from abc import abstractmethod

class EngineBase(ABC):
    """ Base class for sqlalchemy engine"""
    def __init__(self, create_str):
        self._engine = their_create_engine(create_str)
        self._print_debug_msg = False

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

    def _execute(self, *args, **kwargs):
        """ Execute sql commands. """
        if self._print_debug_msg:
            print(args, kwargs)
        return self._engine.execute(*args, **kwargs)

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
    
    @abstractmethod
    def create_table(self, table_name, cols):
        pass
    
    @abstractmethod
    def drop_table(self, table_name):
        pass
    
    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @abstractmethod
    def insert(self, *args, **kwargs):
        pass

    @abstractmethod    
    def delete(self, *args, **kwargs):
        pass