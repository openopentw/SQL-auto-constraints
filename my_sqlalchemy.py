""" Base class for my sqlalchemy. """

from base import EngineBase
import warnings

def create_engine(create_str):
    """ My create engine. """
    return MySqlAlchemy(create_str)

class MySqlAlchemy(EngineBase):
    """class for PyAPI Mysqlalchemy. """
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

        ana_table = self._select(ana_table_name, ['*'])
        for row in ana_table:
            col_name = row['col_name']
            confidence = 1-row['null_cnt']/row['total_cnt']
            col_null_cnt = null_cnt[cols.index(col_name)]
            if confidence > 0.95 and col_null_cnt > 0:
                warnings.warn(f'Values in column {col_name} should not be NULL.')

    def _insert_ana_default(self, ana_table_name, cols, vals):
        ### TO-DO
        pass
        
    
    def _insert_ana_unique(self, ana_table_name, cols, vals):
        """Check UNIQUE constraints"""

        #create columns combinations and list of counts
        from itertools import combinations
        agree_combine = []
        for set_len in range(1,len(cols)+1):
            for combines in combinations(cols , set_len):
                agree_combine.append(list(combines))

        agree_cnt = [0]*len(agree_combine)
        
        #select existing data
        exist_row = self._select(table_name, ['*'])

        #find agree set
        agree_set = []
        #compare with existing rows
        if len(exist_row) > 0 :

            for row in exist_row:
                for new_row in vals:
                    agree_col = []
                    for i in range(len(cols)):
                        if len(set(row[i],new_row[i])) < 2:
                            agree_col.append(cols[i])
                    agree_cnt[agree_combine.index(agree_col)] += 1
                    agree_set.append(agree_col)
        
        #compare between new insert rows
        if len(vals) > 1 :

            for row_num in range(len(vals)-1):
                row_one = vals[row_num]
                for nxt_row_num in range(row_num+1 , len(vals)):
                    row_two = vals[nxt_row_num]
                    agree_col = []
                    for i in range(len(cols)):
                        if len(set(row_one[i],row_two[i])) < 2:
                            agree_col.append(cols[i])
                    agree_cnt[agree_combine.index(agree_col)] += 1
                    agree_set.append(agree_col)



        #save counts to ana_table
        for i, combine in agree_combine:
            if agree_cnt[i] > 0:
                self._update(ana_table_name , 
                            [['agree_cnt' , f'agree_cnt+{agree_cnt[i]}']],
                            f'col_name = "{combine}"')

        #find disagree set
        disagree_set = []
        for columns in agree_set:
            disagree_col = set(cols) - set(columns)
            disagree_set.append(disagree_col)

        #find necessary disagree set
        disagree_set.sort(key=lambda x: len(x))
        nec_disagree_set = []
        while disagree_set:
            columns = disagree_set[0]
            nec_disagree_set.append(columns)
            disagree_set = [ x for x in disagree_set[1:] if not columns.issubset(x) ]

        #transversal

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
