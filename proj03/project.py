"""
Name: Geoffrey Witherington-Perkins
Time To Completion:
Comments:

Sources: Josh's starter code for this project
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            # Get the existing database if there is one
            self.database = _ALL_DATABASES[filename]
        else:
            # Create a new database and add it to the dict of databases
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        # HELPER FUNCTIONS
        # A function defined in another function is only callable in that function
        #   It's defined locally within the outer function
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            cols = []  # column_name : index in the insert statement tuples

            # Next token is either "VALUES", or "(" for the column names
            if tokens[0] == '(':
                tokens.pop(0)
                while True:
                    item = tokens.pop(0)
                    cols.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ',', comma_or_close

            pop_and_check(tokens, "VALUES")

            # Loops until the end of the query to get all the rows that need to be inserted
            while tokens:
                pop_and_check(tokens, "(")
                row_contents = []
                while True:
                    # The row element is first, then a comma or the closing parentheses
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','

                # Call the database insert to insert this row into the correct table
                self.database.insert_into(table_name, cols[:], row_contents)

                # Handling commas between the rows  -->  (), ()
                if tokens:
                    assert tokens.pop(0) == ','

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)

            if tokens:
                # Get the where statement and pass it to the database.delete_from function
                assert tokens.pop(0) == "WHERE"
                self.database.delete_from(table_name, tokens[:])

            else:
                # Delete all the rows in the table
                self.database.delete_from(table_name, [])   # Empty where clause will delete all rows

        def update(tokens):
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")

            set_list = []
            while tokens:
                if tokens[0] == ',':
                    tokens.pop(0)
                elif tokens[0] == "WHERE":
                    break

                col_name = tokens.pop(0)
                tokens.pop(0)
                new_val = tokens.pop(0)
                set_list.append((col_name, new_val))

            where_clause = []
            if tokens:
                # Get the where statement and pass it to the database.delete_from function
                assert tokens.pop(0) == "WHERE"
                self.database.update_table(table_name, set_list, tokens[:])
            else:
                self.database.update_table(table_name, set_list, [])

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """

            pop_and_check(tokens, "SELECT")

            distinct = False
            if tokens[0] == "DISTINCT":
                tokens.pop(0)
                distinct = True

            output_columns = []
            while True:
                col = tokens.pop(0)
                output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table_name = tokens.pop(0)

            # Make sure all the column names don't have the table name prefix on them
            output_columns = [col_name if col_name.find('.') == -1 else col_name[col_name.find('.') + 1:]
                              for col_name in output_columns]

            # WHERE part of the select query
            where_clause = []
            # Just get the tokens of the clause first
            if tokens[0] == 'WHERE':
                tokens.pop(0)
                [where_clause.append(tokens.pop(0)) for _ in range(3)]  # Get next 3 tokens
                if where_clause[-1] == 'NOT':
                    where_clause.append(tokens.pop(0))  # Need to pop one more, since 4 tokens were in the WHERE clause

            # ORDER BY
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []

            # Now get the order by columns
            while True:
                col = tokens.pop(0)
                order_by_columns.append(col)
                if not tokens:
                    break
                pop_and_check(tokens, ",")

            # Doing the same thing as the output columns
            order_by_columns = [col_name if col_name.find('.') == -1 else col_name[col_name.find('.') + 1:]
                                for col_name in order_by_columns]

            return self.database.select(
                output_columns, table_name, order_by_columns, where_clause, distinct)

        # select with join function
            # normal get the columns
        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        else:  # tokens[0] == "SELECT"
            return select(tokens)
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, insert_cols, row_contents):
        """
        Function to handle inserting the row into the appropriate table of the database
        :param table_name: Name of the table being inserted into
        :param insert_cols: The columns that are being inserted
        :param row_contents: The data that's being inserted as a new row
        """
        assert table_name in self.tables
        self.tables[table_name].insert_new_row(insert_cols, row_contents)

    def delete_from(self, table_name, where_clause):
        """
        Function that calls delete_rows on the correct table
        :param table_name: table that things will be deleted from
        :param where_clause: the condition for a row being deleted
        """
        assert table_name in self.tables
        self.tables[table_name].delete_rows(where_clause)

    def update_table(self, table_name, set_list, where_clause):
        """
        Function that calls update_rows on the correct table
        :param table_name: table getting updated
        :param set_list: list of tuples, (col_name, new_value)
        :param where_clause: the "WHERE" condition that determines which rows get updated
        """
        assert table_name in self.tables
        self.tables[table_name].update_rows(set_list, where_clause)

    def select(self, output_columns, table_name, order_by_columns, where_clause, distinct):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns, where_clause, distinct)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        # column_names is a list with elements that come from index 0 of every tuple
        # in the list column_name_type_pairs, and column_types is index 1
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def dequalify_col_name(self, col_name):
        """
        Takes off the qualifying prefix of a column name if there is one
        :param col_name: column name being looked at
        :return: the column name without a prefix, even if there wasn't one to begin with
        """
        if self.name in col_name and col_name[len(self.name) + 1:] in self.column_names:
            return col_name[len(self.name)+1:]
        else:
            return col_name

    def insert_new_row(self, insert_cols, row_contents):
        """
        Function to insert a new row into the table
        :param insert_cols: The columns that are being inserted
        :param row_contents: The actual data that's being added into a new row
        """
        if insert_cols:
            assert len(insert_cols) == len(row_contents), "{} ISN'T THE SAME LENGTH AS {}".format(insert_cols, row_contents)

            # Insert cols was used in the insert statement. Need to fill in the missing cols
            for col_name in self.column_names:
                if col_name not in insert_cols:
                    insert_cols.append(col_name)
                    row_contents.append(None)

        else:
            insert_cols = self.column_names

        # Zip() makes the two lists into one list of tuples, where new_list[0][0] == insert_cols[0] and
        #   new_list[0][1] == row_contents[0]
        # Then, dict() makes that list of tuples into a dictionary
        row = dict(zip(insert_cols, row_contents))
        self.rows.append(row)

    def delete_rows(self, where_clause):
        """
        Deletes rows that satisfy the where clause, deletes all rows if no where clause
        :param where_clause: List of tokens that make up the where clause, delete row if it satisfies this
        """
        if where_clause:
            # Delete the rows that satisfy the where clause
            # Figure out what rows to delete
            rows_to_delete = self.filter_rows_with_where_clause(self.rows, where_clause)

            # Delete the necessary rows
            for row in rows_to_delete:
                self.rows.remove(row)

        else:
            self.rows = []

    def update_rows(self, set_list, where_clause):
        """
        Function to update rows that satisfy the where clause. updates all rows if no where clause
        :param set_list: list of tuples, (col_name, new_value)
        :param where_clause: the "WHERE" condition that determines which rows get updated
        """
        if where_clause:
            # Only update the rows that satisfy the where clause
            # Figure out which rows satisfy the clause
            # Can update the rows in this list because the rows are mutable and thus changing them in this list will
            #   change them in self.rows
            rows_to_update = self.filter_rows_with_where_clause(self.rows, where_clause)

        else:
            # If no where_clause, update all
            rows_to_update = self.rows

        # Update the specified rows
        for row in rows_to_update:
            for col_name, new_val in set_list:
                row[col_name] = new_val

    def select_rows(self, output_columns, order_by_columns, where_clause, distinct):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            # all() checks if all elements in an iterable are true
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            # Itemgetter supports multiple levels of sorting (who knew?)
            # Argument expansion expands an iterable into arguments for a function
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)

        check_columns_exist(order_by_columns)
        rows_list = sort_rows(order_by_columns)

        # is there a where clause?
        #   Run the table.filter_rows... to filter the rows to just the ones that meet the clause
        if where_clause:
            rows_list = self.filter_rows_with_where_clause(rows_list, where_clause)

        if distinct:
            # Get rid of duplicates
            unique_rows = []
            for row in rows_list:
                if row not in unique_rows:
                    unique_rows.append(row)
            
            rows_list = unique_rows

        return generate_tuples(rows_list, expanded_output_columns)

    def filter_rows_with_where_clause(self, rows_list, where_clause):
        """
        Function that takes in a list of rows and a where clause.
        Filters the rows down to only the ones that satisfy the where clause

        :param rows_list: list of rows to be filtered down
        :param where_clause: the clause used to check if a row should be popped or not
        :return: the filtered list
        """

        def perform_comparison(left, operator, right, row):
            """
            Function to decode the operator string and
            :param left:
            :param operator:
            :param right:
            :return:
            """

            def decode_val(val, row):
                """
                Gets a value to be compared to another value
                If the value is a column name, return the value of that column in the current row
                If the value is an integer, return it as an int
                If the value is a float, return it as a float
                If the value is NULL, return NULL instead
                Check for string comparisons too???

                :param row: the row being looked at
                :param val: the val that needs to be 'decoded'
                :return: the 'decoded' value
                """
                # Just return val if it's anything except a string, or if it's a regular string (not a column name)
                if type(val) != str:
                    return val

                # The only things that make it here should be column names. Need to get the value from that column
                val = self.dequalify_col_name(val)  # Get rid of qualifying prefix

                if val not in self.dequalify_col_name(val):
                    return val

                assert val in self.column_names, 'This: {} is somehow not a column name. WAT.'.format(val)
                assert val in row, 'This: {} string is somehow in self.column_names, but not in the row. BIGGER WAT.'

                # If it got through all that, then it must be a valid column name. **Phew**
                return row[val]

            # PERFORM_COMPARISON:
            # Decode left and right first
            left = decode_val(left, row)
            right = decode_val(right, row)

            if len(operator) == 1:
                operator = operator[0]

            # If the operator isn't "IS" or "IS NONE", and one of the values is None:
            if (left is None or right is None) and (operator != 'IS' and operator != ['IS', 'NOT']):
                return False

            if operator == '<':
                return left < right
            if operator == '<=':
                return left <= right
            if operator == '>':
                return left > right
            if operator == '>=':
                return left >= right
            if operator == '=' or operator == 'IS':
                return left == right
            if operator == '!=':
                return left != right

            assert operator == ['IS', 'NOT'], "The operator isn't 'IS NOT'?? It's actually: {}".format(operator)
            return left is not right

        # FILTER_ROWS:
        rows_list = rows_list[:]    # just making sure we're not changing the data structure outside this function
        left = where_clause[0]
        right = where_clause[-1]
        operator = where_clause[1:-1]

        for row in rows_list[:]:
            if not perform_comparison(left, operator, right, row):
                # The where clause is false for this row, get rid of it
                rows_list.remove(row)

        return rows_list

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
    # also passing '.*' to collect_characters to handle qualified column names
    word = collect_characters(query,
                              string.ascii_letters + '_.*' + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    # missing the case where there are more strings later in the query:
    #   Think about taking only the part of the query before the first comma

    assert query[0] == "'"
    query = query[1:]

    # Looping through the query to fix the string escapes and find the final quote
    for i in range(len(query)):
        if query[i] == "'" and query[i+1] == "'":
            # Two single quotes in a row were found, make it only one
            query = query[:i] + query[i+1:]
        elif query[i] == "'":
            # One single quote found, end of the string
            end_quote_index = i
            break

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
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in '<>!':
            if query[1] == '=':
                # Query is <= or another 2-char operators
                tokens.append(query[:2])
                query = query[2:]
            else:
                tokens.append(query[0])
                query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter. \nQuery: {}".format(query))

    return tokens
