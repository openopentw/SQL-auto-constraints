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
                                    ('type', 'varchar(255)'),
                                    ('cnt', 'int'),
                                    ('cnt_toadd', 'int')),
                                   if_not_exists=True)
        self._engine._create_table(self._ana_table_name_2,
                                   (('table_1_name', 'varchar(255)'),
                                    ('col_1_name', 'varchar(255)'),
                                    ('table_2_name', 'varchar(255)'),
                                    ('col_2_name', 'varchar(255)'),
                                    ('in_cnt', 'int'),
                                    ('in_cnt_toadd', 'int')),
                                   if_not_exists=True)

    def drop_ana_table(self):
        """ Drop the analysis table. """
        self._engine._drop_table(self._ana_table_name_1)
        self._engine._drop_table(self._ana_table_name_2)

    def _init_ana_2(self, tab_1, col_1, tab_2, col_2):
        """ Create an entry in _ana_table_name_2. """
        self._engine._insert(self._ana_table_name_2,
                             'all',
                             (tab_1, col_1, tab_2, col_2, 0, 0))

    def before_create_table(self, table_name, cols):
        """ Create table.
        Args:
            table_name: str
            cols: (List/Tuple) [str] w. shape (D, 2)
                e.g.: [['ID', 'int'],
                       ['Name', 'varchar(255)']]
        """
        # with self cols
        if len(cols) > 1:
            for i, (col_name_1, _) in enumerate(cols):
                for col_name_2, _ in cols[i + 1:]:
                    self._init_ana_2(table_name, col_name_1, table_name, col_name_2)
                    self._init_ana_2(table_name, col_name_2, table_name, col_name_1)

        # select other cols
        res = self._engine._select(self._ana_table_name_1,
                                   ('table_name',
                                    'col_name'))
        other_tab_col = [row for row in res]

        # with other cols
        for other_tab, other_col in other_tab_col:
            for col_name, _ in cols:
                self._init_ana_2(table_name, col_name, other_tab, other_col)
                self._init_ana_2(other_tab, other_col, table_name, col_name)

        # insert into ana_1
        for col_name, type_ in cols:
            self._engine._insert(self._ana_table_name_1,
                                 'all',
                                 (table_name, col_name, type_, 0, 0))

    def before_drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._engine._delete(self._ana_table_name_1,
                             f'table_name="{table_name}"')

        self._engine._delete(self._ana_table_name_2,
                             f'table_1_name="{table_name}"'
                             f' OR table_2_name="{table_name}"')

    def _get_in_cnt(self, tab, col, other_tab, other_col):
        """ Get 'in_cnt' of tab-col and other-tab-col from SQL. """
        cond_list = [
            f'table_1_name = "{tab}"',
            f'col_1_name = "{col}"',
            f'table_2_name = "{other_tab}"',
            f'col_2_name = "{other_col}"',
        ]
        cond = ' AND '.join(cond_list)
        cnt = self._engine._select(self._ana_table_name_2,
                                   (f'in_cnt',),
                                   cond=cond)
        cnt = [row for row in cnt]
        cnt = cnt[0][0]
        return cnt

    def _calc_in_cnt(self, tab, col, other_tab, other_col):
        """ Calculate new 'in_cnt' of tab-col and other-tab-col. """
        merge_tab = (f'{tab} AS t'
                     f' LEFT JOIN {other_tab} as o'
                     f' ON t.{col} = o.{other_col}')
        cnt = self._engine._select(f'({merge_tab})',
                                   (f'COUNT(*)',),
                                   f'o.{other_col} IS NOT NULL')
        cnt = [row for row in cnt]
        cnt = cnt[0][0]
        return cnt

    def _update_add_cnt(self, in_cnt, tab, col, other_tab, other_col):
        """ Update cnts_toadd into SQL. """
        cond_list = [
            f'table_1_name = "{tab}"',
            f'col_1_name = "{col}"',
            f'table_2_name = "{other_tab}"',
            f'col_2_name = "{other_col}"',
        ]
        cond = ' AND '.join(cond_list)
        self._engine._update(self._ana_table_name_2,
                             ((f'in_cnt_toadd', f'in_cnt_toadd + {in_cnt}'),),
                             cond=cond)

    def before_insert(self, table_name, cols, vals): # pylint: disable=too-many-locals
        """ Insert into sql.
        Args:
            cols: (List/Tuple) [str]
            vals: (List/Tuple) [ (List/Tuple) [str/int] ]
        """
        self._engine._update(self._ana_table_name_1,
                             (('cnt_toadd',
                               f'cnt_toadd + {len(vals)}'),),
                             cond=f'table_name="{table_name}"')

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
                                   ('table_name', 'col_name', 'cnt'))
        other_tab_col = []
        tab_cnt = {}
        for tab, col, cnt in res:
            other_tab_col.append([tab, col])
            tab_cnt[tab] = cnt

        # merge this_col with other_tab_col & update cnt
        for col in cols:
            for other_tab, other_col in other_tab_col:
                if other_tab != table_name or other_col != col:
                    # in_cnts
                    in_cnt = self._get_in_cnt(table_name, col, other_tab, other_col)
                    new_in_cnt = self._calc_in_cnt(self._ana_table_name_tmp,
                                                   col, other_tab, other_col)
                    not_in_cnt = tab_cnt[table_name] + len(vals) - new_in_cnt
                    self._update_add_cnt(new_in_cnt, table_name, col, other_tab, other_col)

                    # in_cnts
                    in_cnt = self._get_in_cnt(other_tab, other_col, table_name, col)
                    new_in_cnt = self._calc_in_cnt(other_tab, other_col,
                                                   self._ana_table_name_tmp, col)
                    not_in_cnt = tab_cnt[other_tab] - new_in_cnt
                    self._update_add_cnt(new_in_cnt, other_tab, other_col, table_name, col)

        # delete tmp
        self._engine._drop_table(self._ana_table_name_tmp)

        # TODO: jump up warnings if confidence change

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

    def update_cnt(self):
        """ Update counts. """
        self._engine._update(self._ana_table_name_1,
                             (('cnt', f'cnt + cnt_toadd'),
                              ('cnt_toadd', '0'),))
        self._engine._update(self._ana_table_name_2,
                             (('in_cnt', f'in_cnt + in_cnt_toadd'),
                              (f'in_cnt_toadd', '0'),))
