""" Base class for my sqlalchemy. """

from base import EngineBase

def create_engine(create_str):
    """ My create engine. """
    return MySqlAlchemy(create_str)

class MySqlAlchemy(EngineBase):
    """ Base class for my sqlalchemy. """
    def __init__(self, create_str):
        super().__init__(create_str)
        print("You are Using python API to do analyzation.")
        self._constraints = ['null', 'default', 'unique']

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
                if val is None or str(val).lower() == 'null':
                    null_cnt[i] += 1
        for i, col in enumerate(cols):
            if null_cnt[i] > 0:
                self._update(ana_table_name,
                             [['null_cnt', f'null_cnt+{null_cnt[i]}']],
                             f'col_name = "{col}"')
        self._update(ana_table_name,
                    [['total_cnt', f'total_cnt+{len(vals)}']])

    def _insert_ana_default(self, ana_table_name, cols, vals):
        ### TO-DO
        pass
    
    def _insert_ana_unique(self, ana_table_name, cols, vals):
        ## TO-DO
        pass

    # execute sql commands

    def execute(self, *args, **kwargs):
        """ Execute sql commands and check not insert before executing. """
        assert ('insert' not in args[0].lower()), 'Please use insert function instead!'
        return self._execute(*args, **kwargs)

    def insert(self, table_name, cols, vals):
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        self._insert(table_name, cols, vals)

        for cons in self._constraints:
            ana_table_name = self._make_ana_name(table_name, cons)
            eval(f'self._insert_ana_{cons}(ana_table_name, cols, vals)')

    def delete(self, table_name, cond):
        """ Delete some rows. """
        # TODO: update analysis informations

    # modify tables

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

    def drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._drop_table(table_name)
        
        for cons in self._constraints:
            self._drop_table(self._make_ana_name(table_name, cons))
