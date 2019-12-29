""" This is the base class of constraints. """

from abc import ABC
from abc import abstractmethod

class ConstraintBase(ABC):
    """ The base class of constraints. """
    def __init__(self, engine):
        self._engine = engine

    @abstractmethod
    def before_create_table(self, table_name, cols):
        """ Create table.
        Args:
            table_name: str
            cols: (List/Tuple) [str] w. shape (D, 2)
                e.g.: [['ID', 'int'],
                       ['Name', 'varchar(255)']]
        """

    @abstractmethod
    def before_drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """

    @abstractmethod
    def before_insert(self, table_name, cols, vals):
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """

    @abstractmethod
    def before_update(self, table_name, cols, cond=''):
        """ Update rows in a table.
        Args:
            cols: (List/Tuple) [ (List/Tuple) [str/int] ]
            cond: str
        """

    @abstractmethod
    def before_delete(self, table_name, cols, cond=''):
        """ Update rows in a table.
        Args:
            cols: (List/Tuple) [ (List/Tuple) [str/int] ]
            cond: str
        """
