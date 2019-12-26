""" Base class for my sqlalchemy. """

from base import EngineBase

def create_engine(create_str):
    """ My create engine. """
    return MySqlAlchemy(create_str)

class MySqlAlchemy(EngineBase):
    """class for Trigger Mysqlalchemy. """
    def __init__(self, create_str):
        super().__init__(create_str)
        print("You are Using trigger to do analyzation.")
        self._constraints = ['null', 'default', 'unique']

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

    def delete(self, table_name, cond):
        """ Delete some rows. """
        # TODO: update analysis informations

