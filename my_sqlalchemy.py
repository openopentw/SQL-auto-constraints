""" Base class for my sqlalchemy. """

from base import EngineBase
import warnings
from itertools import combinations

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
        # count "NULL"s
        ana_table = self._select(ana_table_name, ['*'])
        null_cnt = {}
        total_cnt = {}
        for row in ana_table:
            col_name = row['col_name']
            null_cnt[col_name] = [row['null_cnt'], False]
            total_cnt[col_name] = row['total_cnt'] + len(vals)
            if col_name in cols:
                for val in vals[cols.index(col_name)]:
                    if val is None or str(val).lower() == 'null':
                        null_cnt[col_name][0] += 1
                        null_cnt[col_name][1] = True
            else:
                null_cnt[col_name] += len(vals)
                null_cnt[col_name][1] = True

        for col, cnt in null_cnt.items():
            confidence = 1-cnt[0]/total_cnt[col]
            if confidence > 0.95 and cnt[1]:
                warnings.warn(f'Values in column {col_name} should not be NULL.')
                while True:
                    reply = input("Do you want to continue the insertion? [Y/n]: ")
                    if reply.lower() == 'y' or reply.lower() == 'yes':
                        break
                    elif reply.lower() == 'n' or reply.lower() == 'no':
                        print("Insertion aborted.")
                        return False
                    else:
                        continue
                break

        # update analysis table
        for col in cols:
            if null_cnt[col][1]:
                self._update(ana_table_name,
                             [['null_cnt', f'{null_cnt[col][0]}']],
                             f'col_name = "{col}"')
        self._update(ana_table_name,
                    [['total_cnt', f'total_cnt+{len(vals)}']])
        return True

    def _insert_ana_default(self, ana_table_name, cols, vals, method='max'):
        default_values = {}
        table_name = ana_table_name.split('_')[1]
        if method == 'max':
            for col_name in cols:
                exe_str = (f'SELECT {col_name}, COUNT(*) AS cnt '
                           f'FROM {table_name} '
                           f'GROUP BY {col_name} ORDER BY cnt DESC LIMIT 1')
                res = self._execute(exe_str).fetchone()
                if res:
                    default_values[col_name] = res["".join(col_name)]
        return True
    
    def _insert_ana_unique(self, ana_table_name, cols, vals):

        """Check UNIQUE constraints"""
        
        #create columns combinations and list of counts
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
                            [['unique_cnt' , f'unique_cnt+{agree_cnt[i]}']],
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
        elm = set.union(*nec_disagree_set)

        elm_combine = []
        for set_len in range(1,len(elm)+1):
            for combines in combinations(elm , set_len):
                elm_combine.append(set(combines))


        for nec_dis in nec_disagree_set:
            all_unique_set = [x for x in elm_combine if len(nec_dis.intersection(x)) > 0]
            elm_combine = all_unique_set

        all_unique_set.sort(key=lambda x: len(x))
        unique_set = []
        while all_unique_set:
            columns = all_unique_set[0]
            unique_set.append(columns)
            all_unique_set = [ x for x in all_unique_set[1:] if not columns.issubset(x) ]

        #warning

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
        success = True
        for cons in self._constraints:
            ana_table_name = self._make_ana_name(table_name, cons)
            success = eval(f'self._insert_ana_{cons}(ana_table_name, cols, vals)')
            if not success:
                break
        
        if success:
            self._insert(table_name, cols, vals)

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
            if cons == 'null':
                self._create_table(ana_table_name, (('col_name', 'varchar(255)'),
                                                (f'{cons}_cnt', 'int'),
                                                ('total_cnt', 'int')))
                ana_cols = []
                for col in cols:
                    ana_cols.append([col[0], 0, 0])
                self._insert(ana_table_name, ('col_name', f'{cons}_cnt', 'total_cnt'), ana_cols)

            elif cons == 'unique':
                self._create_table(ana_table_name, (('col_name', 'varchar(255)'),
                                                    (f'{cons}_cnt', 'int')))
                
                # Extract only column name
                col_names = [v[0] for v in cols]
                all_combine = []
                
                for set_len in range(1, len(col_names)+1):
                    for combines in combinations(col_names , set_len):
                        all_combine.append(["-".join(combines), 0])
                self._insert(ana_table_name, ('col_name', f'{cons}_cnt'), all_combine)

            else:
                self._create_table(ana_table_name, (('col_name', 'varchar(255)'),
                                                (f'{cons}_cnt', 'int'),
                                                ('total_cnt', 'int')))


    def drop_table(self, table_name):
        """ Drop table.
        Args:
            table_name: str
        """
        self._drop_table(table_name)
        
        for cons in self._constraints:
            self._drop_table(self._make_ana_name(table_name, cons))
