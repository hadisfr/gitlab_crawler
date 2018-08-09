import json
import os.path
from sys import stderr
from traceback import format_exc

import MySQLdb
from MySQLdb.cursors import DictCursor


class DBCtrl(object):
    """Database Controller"""
    config_file = os.path.join(os.path.dirname(__file__), "config.json")
    encoding = "utf8mb4"
    DATABASE_NOT_FOUND = 1049
    DUPLICATE_ENTRY = 1062
    SERVER_HAS_GONE = 2006

    def __init__(self):
        try:
            with open(self.config_file) as f:
                self.config = json.load(f)['db']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr, flush=True)
            exit(1)
        try:
            self.connection = MySQLdb.connect(
                user=self.config['user']['username'],
                passwd=self.config['user']['password'],
                host=self.config['host'],
                cursorclass=DictCursor,
                use_unicode=True
            )
            self.connection.set_character_set(self.encoding)
        except MySQLdb.OperationalError as ex:
            print("DB Connection Error: %s\n" % ex, file=stderr, flush=True)
            exit(1)
        try:
            self._open_db()
            self._prepare_tables()
        except MySQLdb.OperationalError as ex:
            print("DB Preparation Error: %s\n" % ex, file=stderr, flush=True)
            exit(1)

    def _get_cursor(self):
        cursor = None
        while not cursor:
            try:
                self.connection.ping(True)
                cursor = self.connection.cursor()
                cursor.execute("SET NAMES %s;" % self.encoding)
                cursor.execute("SET CHARACTER SET %s;" % self.encoding)
                cursor.execute("SET character_set_connection=%s;" % self.encoding)
            except MySQLdb.Error as ex:
                print("Cursor Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        return cursor

    def _open_db(self):
        """Connect to database."""
        cursor = self._get_cursor()
        try:
            cursor.execute("use %s;" % (self.config['name']))
            self.connection.commit()
        except MySQLdb.OperationalError as ex:
            if ex.args[0] == self.DATABASE_NOT_FOUND:
                self.connection.rollback()
                print("creating database %s" + self.config['name'], file=stderr, flush=True)
                cursor.execute("create database %s character set %s;", (self.config['name'], self.encoding))
                cursor.execute("use %s;" % (self.config['name']))
                self.connection.commit()
            else:
                self.connection.rollback()
                raise
        except MySQLdb.Error:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def _prepare_tables(self):
        """Crate missing tables in datatbase."""
        cursor = self._get_cursor()
        try:
            cursor.execute("show tables;")
            found_tables = {list(x.values())[0] for x in cursor.fetchall()}
            for group_of_tables in self.config['tables']:
                missing_tables = set(group_of_tables.keys()).difference(found_tables)
                for table in missing_tables:
                    cursor.execute("create table %s (%s);" % (table, ", ".join(
                        [" ".join((key, value)) for (key, value) in group_of_tables[table].items()]))
                    )
            self.connection.commit()
        except MySQLdb.Error:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def add_row(self, table, values, rerais=False):
        """Add new row to a table of database."""
        cursor = self._get_cursor()
        res = 0
        try:
            res = cursor.execute(
                "insert into %s(%s) values(%s);" % (
                    table, ", ".join(values.keys()),
                    ", ".join(["%s" for value in values])
                ),
                values.values()
            )
            self.connection.commit()
        except Exception as ex:
            self.connection.rollback()
            if rerais:
                raise
            else:
                print("Insert Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        finally:
            cursor.close()
            return res

    def get_rows(self, table, values, rerais=False):
        """Get list of rows from a table of database."""
        res = tuple()
        cursor = self._get_cursor()
        try:
            cursor.execute(
                "select * from %s%s;" %
                (table, ["", " where %s" %
                 " and ".join(["%s=%%s" % key for key in values.keys()])][values is not None and values != {}]),
                tuple(value for value in values.values())
            )
            self.connection.commit()
            res = cursor.fetchall()
        except Exception as ex:
            self.connection.rollback()
            if rerais:
                raise
            else:
                print("Select Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        finally:
            cursor.close()
            return res

    def get_rows_by_query(self, table, query, values, rerais=False):
        """Get list of rows from a table of database by query and a list of values."""
        res = tuple()
        cursor = self._get_cursor()
        try:
            cursor.execute(
                "select * from %s%s;" % (table, ["", " where %s" % query][query is not None and query != ""]),
                values
            )
            self.connection.commit()
            res = cursor.fetchall()
        except Exception as ex:
            self.connection.rollback()
            if rerais:
                raise
            else:
                print("Select Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        finally:
            cursor.close()
            return res

    def update_rows(self, table, conditions, values, rerais=False):
        """Update rows matching conditions in a table of database."""
        cursor = self._get_cursor()
        res = 0
        try:
            res = cursor.execute(
                "update %s set %s%s;" % (
                    table,
                    ", ".join(["%s=%%s" % key for key in values.keys()]),
                    [
                        "",
                        " where %s" % " and ".join(["%s=%%s" % key for key in conditions.keys()])
                    ][conditions is not None and conditions != {}]
                ),
                tuple(value for value in values.values()) + tuple(value for value in conditions.values())
            )
            self.connection.commit()
        except Exception as ex:
            self.connection.rollback()
            if rerais:
                raise
            else:
                print("Update Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        finally:
            cursor.close()
            return res
