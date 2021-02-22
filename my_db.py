from datetime import datetime
from typing import Any, List, Union
import mysql.connector  # type: ignore
from my_log import mylog
from my_json import MyJSON


def get_now_in_mysql(sep_date: str = "/", sep_between_dt: str = " ", sep_time: str = ":") -> str:
    """Generate datetime.now() and return it like MySQL datetime string (STR_TO_DATE( ... )"""
    python_format = "%d{}%m{}%Y{}%H{}%M{}%S".format(
        sep_date, sep_date, sep_between_dt, sep_time, sep_time
    )
    mysql_format = "%d{}%m{}%Y{}%H{}%i{}%s".format(
        sep_date, sep_date, sep_between_dt, sep_time, sep_time
    )
    now = datetime.now()
    python_now: str = now.strftime(python_format)
    return "STR_TO_DATE('%s', '%s')" % (python_now, mysql_format)


def get_mysql_datetime_from_str(text: str, mysql_format: str = "%d/%m/%Y %H:%i:%s") -> str:
    """Return text like MySQL datetime string in format (STR_TO_DATE( ... ). mysql_format must be mysql valid."""
    return "STR_TO_DATE('%s', '%s')" % (text, mysql_format)


def get_mysql_datetime_from_datetime(date_time: datetime) -> str:
    """Return date_time in MySQL datetime string in format (STR_TO_DATE( ... )"""
    python_now: str = date_time.strftime("%d/%m/%Y %H:%M:%S")
    return "STR_TO_DATE('%s', '%s')" % (python_now, "%d/%m/%Y %H:%i:%s")


class MyDbMySql:
    """Class for working with MySql Database.
    Example:
     - mydb = MyDbMySql("my_db_mysql.ini")
     - mydb.connect()
     - mydb.execute_query("INSERT INTO AnyTable(AnyCol) VALUES(AnyVal);")
     - mydb.execute_commit()
     - mydb.get_select("SELECT * FROM AnyTable");
     - mydb.disconnect()"""

    def __init__(self, input_file_name: str):
        self.input_file_name = input_file_name
        self.is_connected: bool = False  # information about connection

    def connect(self):
        """Create connection to MySql database py params in input_file_name"""
        mylog.enter()
        if self.is_connected:
            mylog.warning("Database is connectet. For another connect must be disconnected.")
        else:
            myparam = MyJSON(self.input_file_name)
            host: str = myparam.get_value("host")
            port: str = myparam.get_value("port")
            user: str = myparam.get_value("user")
            password: str = myparam.get_value("password")
            database: str = myparam.get_value("database")
            try:
                self.dbConnector = mysql.connector.connect(user=user,
                                                           password=password,
                                                           host=host,
                                                           port=port,
                                                           database=database,
                                                           buffered=True)  # no error when no data for cursor
                self.is_connected = True
                mylog.info("Database connected.")
            except mysql.connector.errors.Error as err:
                mylog.error(err, "Check params in ini file: %s" % (self.input_file_name))
                raise
        mylog.exit()

    def disconnect(self):
        """Disconnect from MySql database."""
        mylog.enter()
        if not self.is_connected:
            mylog.warning("Database can't be disconnected, because it's not connected.")
        else:
            try:
                self.dbConnector.close()
                self.is_connected = False
                mylog.info("Database disconnected")
            except mysql.connector.errors.Error as err:
                mylog.error("Database disconnect error", err)
                raise
            mylog.exit()

    def execute_query(self, sql_query: str):
        """Execute non-select sql query"""
        mylog.enter()
        mylog.info("sql_query:", sql_query)
        try:
            self.mycursor = self.dbConnector.cursor()
            self.mycursor.execute(sql_query)
            mylog.info("affected_rows", self.mycursor.rowcount)
            mylog.exit()
        except Exception as err:
            mylog.error(err)
            raise
        finally:
            self.mycursor.close()

    def execute_commit(self):
        mylog.enter()
        self.execute_query("COMMIT")
        mylog.info('Database commited.')
        mylog.exit()

    def execute_rollback(self):
        mylog.enter()
        self.execute_query("ROLLBACK")
        mylog.info('Database rollbacked.')
        mylog.exit()

    def get_select(self, sql_select: str) -> List[tuple]:
        """Execute sql select and return value of select like list of tuples."""
        mylog.enter()
        mylog.info("sql_select:", sql_select)
        sql_result: List[tuple] = []
        try:
            self.mycursor = self.dbConnector.cursor()
            self.mycursor.execute(sql_select)
            sql_result = self.mycursor.fetchall()  # non-select query will crash here
            mylog.info("affected_rows", self.mycursor.rowcount)
        except Exception as err:
            mylog.error(err)
            raise
        finally:
            self.mycursor.close()
        mylog.info("sql_result: ", sql_result)
        mylog.exit()
        return sql_result

    def get_last_id(self, table, column) -> int:
        """Return last ID after processed executed insert. If is inserted more rows, return ID for first inserted row."""
        mylog.enter()
        sql: str = "SELECT %s FROM %s WHERE %s = LAST_INSERT_ID()" % (
            column,
            table,
            column,
        )
        res = self.get_select(sql_select=sql)
        if not res:
            mylog.error(
                "ID was not inserted before calling this method or column is not primary key."
            )
            raise
        else:
            last_id: int = int(res[0][0])
            mylog.info("last_id:", last_id)
        mylog.exit()
        return last_id

    def __get_sql_conversion(self, value: Any) -> str:
        """Prepare conversion from Python variables to sql variables."""
        ignored_values: list = ["NULL"]  # this values will not converted like sql string
        ignored_parts: list = ["STR_TO_DATE"]  # if is this part in string, value will not converted to sql string
        val: str = str(value)
        if isinstance(value, str):
            if value.upper() not in ignored_values:
                is_in_ignored_parts = False
                for part in ignored_parts:
                    if value.upper().find(part) != -1:
                        is_in_ignored_parts = True
                        break
                if not is_in_ignored_parts:
                    val = "'%s'" % (str(value))
        return val

    def __get_sql_insert(self, table: str, column_list: list, value_list: Union[list, List[list]]) -> str:
        """Create and execute sql insert. value_list can be one dim or two dim (for more inserted rows)
        Example for different varables:
        table='Persons' column_list=['Name', 'Age']; value_list=[['Bob', 18], ['John', 'null'], ['Tom', 'unknown']]
        Result: INSERT INTO Persons
          (Name, Age)
        VALUES
          ('Bob',18), ('John',null), ('Tom','unknown')"""
        # columns
        mylog.enter()
        columns: str = "\t(" + ", ".join(column_list) + ")"  # tab is for nice view

        # if is one dim list, convert to 2 dim (its processing like 2 dim)
        if not isinstance(value_list[0], list):
            value_list = list([value_list[:]])

        # preparing values (added tabs and new_lines for better future view)
        prep_value: list = []
        for val_list in value_list:
            prep_list: list = []
            for val in val_list:
                prep_list.append(self.__get_sql_conversion(val))
            row: str = "\t(" + ",".join(prep_list) + "),\n"
            prep_value.append(row)

        values: str = "".join(prep_value)
        values = values[:-2]  # cut new line and comma char at the end
        # create sql insert query
        sql: str = """\nINSERT INTO %s\n%s\nVALUES\n%s""" % (table, columns, values)
        mylog.exit()
        return sql

    def execute_insert(self, table: str, column_list: list, value_list: Union[list, List[list]]):
        mylog.enter()
        mylog.info("column_list:", column_list)
        mylog.info("value_list", value_list)
        sql: str = self.__get_sql_insert(
            table=table, column_list=column_list, value_list=value_list
        )
        self.execute_query(sql_query=sql)
        mylog.exit()

    def execute_update_finishedat(
        self, table: str, sql_datetime, id_column_name: str, id_column_val: str
    ):
        """Execute: Update table SET FinishedAt=sql_datetime WHERE where_id_column_name=id_column_val"""
        mylog.enter()
        sql: str = """\nUPDATE %s\n \tSET FinishedAt = %s\n \tWHERE %s = %s""" % (
            table,
            sql_datetime,
            id_column_name,
            id_column_val,
        )
        mylog.info()
        self.execute_query(sql_query=sql)
        mylog.exit()


class MyTableDef:
    """Creating objects from table definition. Must be written in json format in this form:
    {
      "BET_Link": [
          "CREATE TABLE BET_Link(",
                          "LinkID INT AUTO_INCREMENT PRIMARY KEY,",
                          "ScanProcessID INT NULL,",
                          "ImportType VARCHAR(30) NULL,",
                          "CreatedAt TIMESTAMP NULL);"
          ]
    }
    --
    Working with object:
    tb = MyTableDef(table_name="BET_Link")
    tb.print_table_definition()
    notnull_cols = tb.get_notnull_cols_without_pk()
    all_cols = tb.get_all_columns()
    table_name = tb.get_table_name()
    """

    def __init__(self, table_name: str, input_file_name: str = "bet_files/tables.ini"):
        self.table_name: str = table_name
        self.input_file_name: str = input_file_name
        self.json_table_value: list = []

    def __load_json(self):
        myjson = MyJSON(input_file_name=self.input_file_name)
        self.json_table_value = myjson.get_value(self.table_name)

    def print_table_definition(self):
        """Print table definition for processing in sql browser"""
        self.__load_json()
        table_definition: str = ""
        for index, row in enumerate(self.json_table_value):
            if index == 0:
                table_definition += row + "\n"
            else:
                table_definition += "\t" + row + "\n"

        print(table_definition)

    def __get_specific_columns(self, ignored_col_spec: list = [], require_list: list = []) -> List[str]:
        """Return column list with specification in ignored_col_spec, require_list"""
        columns: List[str] = []
        for row in self.json_table_value:
            row = row.replace("\t", " ")
            is_in_ignore = False
            is_in_require = False
            if ignored_col_spec:
                for ign in ignored_col_spec:
                    if row.upper().find(ign) != -1:
                        is_in_ignore = True
                        break

            if require_list:
                for req in require_list:
                    if row.upper().find(req) != -1:
                        is_in_require = True
                        break
            else:
                is_in_require = True

            if not is_in_ignore:
                if is_in_require:
                    column_name = row[: row.find(" ")]
                    columns.append(column_name)

        return columns

    def get_notnull_cols_without_pk(self) -> List[str]:
        """Return all not null columns without primary key"""
        self.__load_json()
        ignore: list = ["AUTO_INCREMENT", "PRIMARY KEY", "NOW()", "CREATE TABLE"]
        require = ["NOT NULL"]
        return self.__get_specific_columns(
            ignored_col_spec=ignore, require_list=require
        )

    def get_nopk_cols(self) -> List[str]:
        """Return all non pk columns"""
        self.__load_json()
        ignore: list = ["AUTO_INCREMENT", "PRIMARY KEY", "NOW()", "CREATE TABLE"]
        return self.__get_specific_columns(ignored_col_spec=ignore)

    def get_all_columns(self) -> List[str]:
        self.__load_json()
        ignore: list = ["CREATE TABLE"]
        return self.__get_specific_columns(ignored_col_spec=ignore)

    def get_table_name(self) -> str:
        self.__load_json()
        row_table: str = self.json_table_value[0]
        start_str = "CREATE TABLE "
        table_name = row_table[
            row_table.find(start_str) + len(start_str) : row_table.find("(")
        ]
        return table_name

    def get_pk_col_name(self) -> str:
        """Return primary key column name"""
        self.__load_json()
        require: list = ["PRIMARY KEY"]
        row_with_pk = self.__get_specific_columns(require_list=require)
        pk: str = "".join(row_with_pk)
        return pk


if __name__ == "__main__":
    # print('test')
    print('test')
    # mydb = MyDbMySql(input_file_name="db_settings.ini")
    # mydb.connect()
    # mydb.get_select('SELECT * FROM Persons')
    # # mydb.execute_query('INSERT INTO Persons(Age) VALUES(15)')
    # # mydb.get_last_id(table='Persons', column='Personid')
    # mydb.execute_insert(table='Persons', column_list=['Age'], value_list=[[18], ['null'], [20]])
    # mydb.execute_commit()
    # mydb.disconnect()

    # tb = MyTableDef(table_name="Persons")
    # tb.print_table_definition()
    # print(tb.get_notnull_cols_without_pk())
    # print(tb.get_all_columns())
    # print(tb.get_table_name())
    # print(tb.get_nopk_cols())
