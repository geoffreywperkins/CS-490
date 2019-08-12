"""
Name: Geoffrey Witherington-Perkins
Time To Completion: 10 hrs
Comments:

Sources: Josh's project 3 solution
"""
import string
from operator import itemgetter
from collections import namedtuple
import itertools
from copy import deepcopy

_ALL_DATABASES = {}


# Kind of like a struct in C++, namedtuples are like classes without methods. Only member variables
WhereClause = namedtuple("WhereClause", ["col_name", "operator", "constant"])
UpdateClause = namedtuple("UpdateClause", ["col_name", "constant"])
FromJoinClause = namedtuple("FromJoinClause", ["left_table_name",
                                               "right_table_name",
                                               "left_join_col_name",
                                               "right_join_col_name"])


def reset():
    """
    Function for running tests locally. Clears all the databases
    """
    _ALL_DATABASES.clear()


class Connection(object):

    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

        self.transaction_open = False
        self.transaction_lock = ''
        self.modified_database = self.database  # The copy of the DB that will get modified

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def get_locks_for_writing():
            # Get the required locks
            if self.transaction_open:
                # If the transaction has a shared lock, release it
                if self.transaction_lock == "S":
                    self.database.release_shared_lock()
                # Get the necessary locks
                if not self.transaction_lock or self.transaction_lock == "S":
                    self.transaction_lock = "R"
                    self.database.get_reserved_lock()
            else:
                # In autocommit, just check if exclusive locks are available
                self.database.get_exclusive_lock()
                self.database.release_exclusive_lock()

        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """

            get_locks_for_writing()

            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")

            if_not_exists = False
            if tokens[0] == 'IF':
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                if_not_exists = True

            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)

                # Using the new QualifiedColumnName class
                qual_col_name = QualifiedColumnName(column_name, table_name)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((qual_col_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','

            # Add the new table to the database
            self.modified_database.create_new_table(table_name, column_name_type_pairs, if_not_exists)

        def drop_table(tokens):
            """
            Drops the specified table
            """
            get_locks_for_writing()

            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")

            if_exists = False
            if tokens[0] == "IF":
                tokens.pop(0)
                tokens.pop(0)
                if_exists = True

            table_name = tokens.pop(0)

            self.modified_database.drop_table(table_name, if_exists)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            def get_comma_seperated_contents(tokens):
                contents = []
                pop_and_check(tokens, "(")
                while True:
                    item = tokens.pop(0)
                    contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        return contents
                    assert comma_or_close == ',', comma_or_close

            get_locks_for_writing()

            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)

            # Get the column names if specified:
            if tokens[0] == "(":
                col_names = get_comma_seperated_contents(tokens)
                qual_col_names = [QualifiedColumnName(col_name, table_name)
                                  for col_name in col_names]
            else:
                qual_col_names = None
            pop_and_check(tokens, "VALUES")
            while tokens:
                row_contents = get_comma_seperated_contents(tokens)
                if qual_col_names:
                    assert len(row_contents) == len(qual_col_names)
                self.modified_database.insert_into(table_name,
                                                   row_contents,
                                                   qual_col_names=qual_col_names)
                if tokens:
                    pop_and_check(tokens, ",")

        def get_qualified_column_name(tokens):
            """
            Returns a QualifiedColumnName object with col_name and table_name from the column name at
            the front of the tokens list
            """
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)

        def update(tokens):
            get_locks_for_writing()

            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            update_clauses = []
            while tokens:
                qual_name = get_qualified_column_name(tokens)
                if not qual_name.table_name:
                    qual_name.table_name = table_name
                pop_and_check(tokens, '=')
                constant = tokens.pop(0)
                update_clause = UpdateClause(qual_name, constant)
                update_clauses.append(update_clause)
                if tokens:
                    if tokens[0] == ',':
                        tokens.pop(0)
                        continue
                    elif tokens[0] == "WHERE":
                        break

            where_clause = get_where_clause(tokens, table_name)

            self.modified_database.update(table_name, update_clauses, where_clause)

        def delete(tokens):
            get_locks_for_writing()

            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")

            table_name = tokens.pop(0)
            where_clause = get_where_clause(tokens, table_name)
            self.modified_database.delete(table_name, where_clause)

        def get_where_clause(tokens, table_name):
            """
            Gets the where clause that's at the front of the list of tokens
            :return: a WhereClause named tuple
            """

            # If there's no WHERE clause:
            if not tokens or tokens[0] != "WHERE":
                return None
            tokens.pop(0)
            qual_col_name = get_qualified_column_name(tokens)

            # Filling in the table name if it wasn't specified
            if not qual_col_name.table_name:
                qual_col_name.table_name = table_name

            operators = {">", "<", "=", "!=", "IS"}
            found_operator = tokens.pop(0)
            assert found_operator in operators
            if tokens[0] == "NOT":
                tokens.pop(0)
                found_operator += " NOT"
            constant = tokens.pop(0)

            # IS or IS NOT are the only operators allowed for comparing to None
            if constant is None:
                assert found_operator in {"IS", "IS NOT"}
            if found_operator in {"IS", "IS NOT"}:
                assert constant is None
            return WhereClause(qual_col_name, found_operator, constant)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """

            def get_from_join_clause(tokens):
                """
                Function to extract the JOIN clause
                """
                left_table_name = tokens.pop(0)
                if tokens[0] != "LEFT":
                    # Only one table
                    return FromJoinClause(left_table_name, None, None, None)
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                right_table_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                left_col_name = get_qualified_column_name(tokens)
                pop_and_check(tokens, "=")
                right_col_name = get_qualified_column_name(tokens)

                # There's a join happening, fill in all the specified info
                return FromJoinClause(left_table_name,
                                      right_table_name,
                                      left_col_name,
                                      right_col_name)

            pop_and_check(tokens, "SELECT")

            # Making sure it can get the required locks
            if self.transaction_open:
                if not self.transaction_lock:
                    self.database.get_shared_lock()
                    self.transaction_lock = "S"
            else:
                self.database.get_shared_lock()
                self.database.release_shared_lock()

            # Flag for if the SELECT statement is DISTINCT
            is_distinct = tokens[0] == "DISTINCT"
            if is_distinct:
                tokens.pop(0)

            output_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                output_columns.append(qual_col_name)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            # FROM or JOIN
            from_join_clause = get_from_join_clause(tokens)
            table_name = from_join_clause.left_table_name

            # WHERE
            where_clause = get_where_clause(tokens, table_name)

            # ORDER BY
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                order_by_columns.append(qual_col_name)
                if not tokens:
                    break
                pop_and_check(tokens, ",")

            # Calling select on the database, passing all the necessary info
            return self.modified_database.select(
                output_columns,
                order_by_columns,
                from_join_clause=from_join_clause,
                where_clause=where_clause,
                is_distinct=is_distinct)

        def begin_transaction(tokens):
            """
            Function that handles a "BEGIN TRANSACTION" instruction
                Throws error if there's already a transaction open
                Makes a local copy of the DB
                Sets the transaction_open flag to True
            """
            assert not self.transaction_open
            pop_and_check(tokens, "BEGIN")

            if tokens[0] != "TRANSACTION":
                # Get the locks if there's a transaction mode and it's immediate or deferred
                transaction_mode = tokens.pop(0)
                if transaction_mode == "IMMEDIATE":
                    self.database.get_reserved_lock()
                    self.transaction_lock = "R"
                elif transaction_mode == "EXCLUSIVE":
                    self.database.get_exclusive_lock()
                    self.transaction_lock = "E"

            pop_and_check(tokens, "TRANSACTION")

            # Copy the database
            self.modified_database = Database(self.database.filename)
            self.modified_database.tables = deepcopy(self.database.tables)

            # Set the transaction open flag to True
            self.transaction_open = True

        def commit_transaction(tokens):
            """
            Function that handles a "COMMIT TRANSACTION" instruction
                Throws an error if there's no open transaction
                Upgrade to an exclusive lock
                Commits the transaction's changes to the original copy of the DB
                    originalDB.tables = localDB.tables
                Set transaction_open flag to False
            """
            assert self.transaction_open
            pop_and_check(tokens, "COMMIT")
            pop_and_check(tokens, "TRANSACTION")

            # Upgrade to exclusive lock if the lock is reserved
            if self.transaction_lock == "R":
                self.database.release_reserved_lock()
                self.database.get_exclusive_lock()
                self.transaction_lock = "E"

            # Release the locks
            if self.transaction_lock == "E":
                self.database.release_exclusive_lock()
            elif self.transaction_lock == "S":
                self.database.release_shared_lock()

            self.database.tables = self.modified_database.tables
            self.modified_database = self.database
            self.transaction_lock = ""
            self.transaction_open = False

        def rollback_transaction(tokens):
            """
            Function that handles a "ROLLBACK TRANSACTION" instruction
                Throws an error if there's no open transaction
                Gets rid of the local DB
                Sets transaction_open flag to False
            """
            assert self.transaction_open
            pop_and_check(tokens, "ROLLBACK")
            pop_and_check(tokens, "TRANSACTION")
            self.modified_database = self.database

            # Release the locks
            if self.transaction_lock == "E":
                self.database.release_exclusive_lock()
            elif self.transaction_lock == "R":
                self.database.release_reserved_lock()
            elif self.transaction_lock == "S":
                self.database.release_shared_lock()

            self.transaction_lock = ""
            self.transaction_open = False

        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "DROP":
            drop_table(tokens)
            return []
        elif tokens[0] == "BEGIN":
            begin_transaction(tokens)
            return []
        elif tokens[0] == "COMMIT":
            commit_transaction(tokens)
            return []
        elif tokens[0] == "ROLLBACK":
            rollback_transaction(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        else:
            raise AssertionError(
                "Unexpected first word in statements: " + tokens[0])

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=0.1, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class QualifiedColumnName:
    """
    Class to store qualified column names
    Has the name of the column and the name of the table
    """

    def __init__(self, col_name, table_name=None):
        self.col_name = col_name
        self.table_name = table_name

    def __str__(self):
        return "QualifiedName({}.{})".format(
            self.table_name, self.col_name)

    def __eq__(self, other):
        same_col = self.col_name == other.col_name
        if not same_col:
            return False
        both_have_tables = (self.table_name is not None and
                            other.col_name is not None)
        if not both_have_tables:
            return True
        return self.table_name == other.table_name

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        """
        This function allows a hash key to be given to a QualifiedColumnName object
        Per the Python documentation, you should pack all the necessary data members for testing equivalence into a
        tuple, and use the built-in hash() function to hash the tuple
        """
        return hash((self.col_name, self.table_name))

    def __repr__(self):
        return str(self)


class Database:

    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.locks = {"S": 0, "R": 0, "E": 0}

    def get_shared_lock(self):
        """
        Gets a shared lock, raises an error if it can't get the lock
        """
        assert not (self.locks["E"])
        self.locks["S"] += 1

    def get_reserved_lock(self):
        """
        Gets a reserved lock, raises an exception if it can't get the lock
        """
        assert not (self.locks["R"] or self.locks["E"])
        self.locks["R"] += 1

    def get_exclusive_lock(self):
        """
        Gets an exclusive lock, raises exception if it can't get the lock
        """
        assert not (self.locks["S"] or self.locks["R"] or self.locks["E"])
        self.locks["E"] += 1

    def release_shared_lock(self):
        assert self.locks["S"]
        self.locks["S"] -= 1

    def release_reserved_lock(self):
        assert self.locks["R"]
        self.locks["R"] -= 1

    def release_exclusive_lock(self):
        assert self.locks["E"]
        self.locks["E"] -= 1

    def create_new_table(self, table_name, column_name_type_pairs, if_not_exists):
        if not if_not_exists:
            assert table_name not in self.tables
        else:
            # If the query said "IF NOT EXISTS", and the table is already there, don't make the new table
            if table_name in self.tables:
                return
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def drop_table(self, table_name, if_exists):
        if if_exists and table_name not in self.tables:
            return
        self.tables.pop(table_name)

    def insert_into(self, table_name, row_contents, qual_col_names=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, qual_col_names=qual_col_names)
        return []

    def update(self, table_name, update_clauses, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(update_clauses, where_clause)

    def delete(self, table_name, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete(where_clause)

    def select(self, output_columns, order_by_columns,
               from_join_clause,
               where_clause=None, is_distinct=False):
        assert from_join_clause.left_table_name in self.tables
        if from_join_clause.right_table_name:
            assert from_join_clause.right_table_name in self.tables
            left_table = self.tables[from_join_clause.left_table_name]
            right_table = self.tables[from_join_clause.right_table_name]

            # Chain makes a new list, like doing list[0].extend(list[1]).extend(.....
            all_columns = itertools.chain(
                zip(left_table.column_names, left_table.column_types),
                zip(right_table.column_names, right_table.column_types))

            left_col = from_join_clause.left_join_col_name
            right_col = from_join_clause.right_join_col_name

            # Making a temporary table that'll be filled with all the data from the two joining tables
            join_table = Table("", all_columns)

            # List of the new rows with the data from both tables
            combined_rows = []
            for left_row in left_table.rows:    # Looping over all the left rows
                left_value = left_row[left_col]
                found_match = False
                for right_row in right_table.rows:  # Looping over all the right rows
                    right_value = right_row[right_col]

                    # Look for where there's a match. Can't join on NULL
                    if left_value is None:
                        break
                    if right_value is None:
                        continue
                    if left_row[left_col] == right_row[right_col]:
                        # Build a new row if there's a match
                        new_row = dict(left_row)
                        new_row.update(right_row)
                        combined_rows.append(new_row)
                        found_match = True
                        continue
                if left_value is None or not found_match:
                    # No match found, the join is all the left values, and all NULL for the right
                    new_row = dict(left_row)
                    new_row.update(zip(right_row.keys(),
                                       itertools.repeat(None)))
                    combined_rows.append(new_row)

            join_table.rows = combined_rows
            table = join_table
        else:
            table = self.tables[from_join_clause.left_table_name]
        return table.select_rows(output_columns, order_by_columns,
                                 where_clause=where_clause,
                                 is_distinct=is_distinct)


class Table:

    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents, qual_col_names=None):
        if not qual_col_names:  # If the column names aren't specified, the values will be in the order of the cols
            qual_col_names = self.column_names
        assert len(qual_col_names) == len(row_contents)

        # Zip takes the two lists, makes one list of tuples, dict makes a dictionary from the list of tuples
        row = dict(zip(qual_col_names, row_contents))

        for null_default_col in set(self.column_names) - set(qual_col_names):
            row[null_default_col] = None
        self.rows.append(row)

    def update(self, update_clauses, where_clause):
        for row in self.rows:
            if self._row_match_where(row, where_clause):
                for update_clause in update_clauses:
                    row[update_clause.col_name] = update_clause.constant

    def delete(self, where_clause):
        self.rows = [row for row in self.rows
                     if not self._row_match_where(row, where_clause)]

    def _row_match_where(self, row, where_clause):
        """
        Checking if there's a match in the where clause
        """
        if not where_clause:
            return True
        new_rows = []
        value = row[where_clause.col_name]

        op = where_clause.operator
        cons = where_clause.constant
        if ((op == "IS NOT" and (value is not cons)) or
                (op == "IS" and value is cons)):
            return True

        if value is None:
            return False

        if ((op == ">" and value > cons) or
            (op == "<" and value < cons) or
            (op == "=" and value == cons) or
                (op == "!=" and value != cons)):
            return True
        return False

    def select_rows(self, output_columns, order_by_columns,
                    where_clause=None, is_distinct=False):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col.col_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names
                       for col in columns)

        def ensure_fully_qualified(columns):
            for col in columns:
                if col.table_name is None:
                    col.table_name = self.name

        def sort_rows(rows, order_by_columns):
            return sorted(rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        def remove_duplicates(tuples):
            seen = set()
            uniques = []
            for row in tuples:
                if row in seen:
                    continue
                seen.add(row)
                uniques.append(row)
            return uniques

        expanded_output_columns = expand_star_column(output_columns)

        check_columns_exist(expanded_output_columns)
        ensure_fully_qualified(expanded_output_columns)
        check_columns_exist(order_by_columns)
        ensure_fully_qualified(order_by_columns)

        filtered_rows = [row for row in self.rows
                         if self._row_match_where(row, where_clause)]
        sorted_rows = sort_rows(filtered_rows, order_by_columns)

        list_of_tuples = generate_tuples(sorted_rows, expanded_output_columns)
        if is_distinct:
            return remove_duplicates(list_of_tuples)
        return list_of_tuples


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    if (query[0] == "'"):
        delimiter = "'"
    else:
        delimiter = '"'
    query = query[1:]
    end_quote_index = query.find(delimiter)
    while query[end_quote_index + 1] == delimiter:
        # Remove Escaped Quote
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index = query.find(delimiter, end_quote_index + 1)
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] in "(),;*.><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in {"'", '"'}:
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError(
                "Query didn't get shorter. query = {}".format(query))

    return tokens
