# pylint: disable=protected-access
""" This is the class for providing referencial constraints. """

# import warnings

from constraints.constraint_base import ConstraintBase

class Referencial(ConstraintBase):
    """ The class of referencial constraint. """
    def __init__(self, engine):
        super().__init__(engine)
        self._ana_table_name_1 = '_reference_analysis_1'
        self._ana_table_name_2 = '_reference_analysis_2'
        self._ana_table_name_tmp = '_reference_analysis_tmp'
        self._engine._create_table(self._ana_table_name_1,
                                   (('table_name', 'varchar(255)'),
                                    ('col_name', 'varchar(255)'),
                                    ('type', 'varchar(255)')),
                                   if_not_exists=True)
        self._engine._create_table(self._ana_table_name_2,
                                   (('table_1_name', 'varchar(255)'),
                                    ('col_1_name', 'varchar(255)'),
                                    ('table_2_name', 'varchar(255)'),
                                    ('col_2_name', 'varchar(255)'),
                                    ('in_cnt', 'int')),
                                   if_not_exists=True)

    def drop_ana_table(self):
        """ Drop the analysis table. """
        self._engine._drop_table(self._ana_table_name_1)
        self._engine._drop_table(self._ana_table_name_2)

    def before_create_table(self, table_name, cols):
        """ Create table.
        Args:
            table_name: str
            cols: (List/Tuple) [str] w. shape (D, 2)
                e.g.: [['ID', 'int'],
                       ['Name', 'varchar(255)']]
        """
        # select other cols
        res = self._engine._select(self._ana_table_name_1,
                                   ('table_name',
                                    'col_name'))
        other_tab_col = [row for row in res]

        # insert into ana_1
        for col_name, type_ in cols:
            self._engine._insert(self._ana_table_name_1,
                                 'all',
                                 (table_name, col_name, type_))

        # with self cols
        if len(cols) > 1:
            for i, (col_name_1, _) in enumerate(cols):
                for col_name_2, _ in cols[i + 1:]:
                    self._engine._insert(self._ana_table_name_2,
                                         'all',
                                         (table_name,
                                          col_name_1,
                                          table_name,
                                          col_name_2,
                                          0))

        # with other cols
        if len(other_tab_col) > 1:
            for other_tab, other_col in other_tab_col:
                for col_name, _ in cols:
                    self._engine._insert(self._ana_table_name_2,
                                         'all',
                                         (table_name,
                                          col_name,
                                          other_tab,
                                          other_col,
                                          0))

    def before_drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._engine._delete(self._ana_table_name_1,
                             f'table_name="{table_name}"')

        self._engine._delete(self._ana_table_name_2,
                             f'table_1_name="{table_name}"'
                             f'OR table_2_name="{table_name}"')

    def _before_insert_in_cnt(self, tab, col, other_tab, other_col):
        """ Update 'in_cnt' of tab-col and other-tab-col. """
        # get in count
        merge_tab = (f'{self._ana_table_name_tmp} AS t'
                     f' LEFT JOIN {other_tab} as o'
                     f' ON t.{col} = o.{other_col}')
        cnt = self._engine._select(f'({merge_tab})',
                                   (f'COUNT(*)',),
                                   f'o.{other_col} IS NOT NULL')
        cnt = [row for row in cnt]
        cnt = cnt[0][0]

        # update in count
        for tab_1, tab_2 in [[1, 2], [2, 1]]:
            cond_list = [
                f'table_{tab_1}_name = "{tab}"',
                f'col_{tab_1}_name = "{col}"',
                f'table_{tab_2}_name = "{other_tab}"',
                f'col_{tab_2}_name = "{other_col}"',
            ]
            cond = ' AND '.join(cond_list)
            self._engine._update(self._ana_table_name_2,
                                 (('in_cnt',
                                   f'in_cnt + {cnt}'),),
                                 cond=cond)

    def before_insert(self, table_name, cols, vals): # pylint: disable=too-many-locals
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        # find create types for this table
        res = self._engine._select(self._ana_table_name_1,
                                   ('col_name', 'type'),
                                   f'table_name="{table_name}"')
        this_col_type = [row for row in res]

        # create tmp & insert
        self._engine._create_table(self._ana_table_name_tmp,
                                   this_col_type,
                                   tmp=True)
        self._engine._insert(self._ana_table_name_tmp,
                             cols,
                             vals)

        # select other cols
        res = self._engine._select(self._ana_table_name_1,
                                   ('table_name', 'col_name'))
        other_tab_col = [row for row in res]

        # merge this_col with other_tab_col & update cnt
        for col in cols:
            for other_tab, other_col in other_tab_col:
                if other_tab != table_name or other_col != col:
                    self._before_insert_in_cnt(table_name,
                                               col,
                                               other_tab,
                                               other_col)

        # delete tmp
        self._engine._drop_table(self._ana_table_name_tmp)

        # TODO: calculate confidence
        # TODO: check if the confidence is good
        return True

    def before_update(self, table_name, cols, cond=''):
        """ Update rows in a table.
        Args:
            cols: (List/Tuple) [ (List/Tuple) [str/int] ]
            cond: str
        """

    def before_delete(self, table_name, cols, cond=''):
        """ Update rows in a table.
        Args:
            cols: (List/Tuple) [ (List/Tuple) [str/int] ]
            cond: str
        """
