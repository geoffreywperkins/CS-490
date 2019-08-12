from pprint import pprint
import unittest
from project import connect, reset



def check(conn, sql_statement, expected):
    print("SQL: " + sql_statement)
    result = conn.execute(sql_statement)
    result_list = list(result)

    print("expected:")
    pprint(expected)
    print("student: ")
    pprint(result_list)
    assert expected == result_list


class TestsProject4(unittest.TestCase):
    # NOTE: This function is called after each test is ran (often referred to
    # as the cleanup function). You may need to make a function that resets
    # the state of your program (i.e remove all databases and clear all locks)
    # in order to run the tests one after the other, otherwise your program
    # may throw errors (i.e trying to create a table that shouldn't exist but
    # does due to a previous test).
    def tearDown(self):
        # reset_state()
        reset()

    def check(self, conn, sql_statement, expected):
        print("SQL: " + sql_statement)
        result = conn.execute(sql_statement)
        result_list = list(result)

        print("expected:")
        pprint(expected)
        print("student: ")
        pprint(result_list)
        assert expected == result_list

    def test_regression(self):
        conn = connect("test.db")
        conn.execute(
            "CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
        conn.execute(
            "CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
        conn.execute(
            "INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
        conn.execute(
            "INSERT INTO pets (species, name) VALUES ('Rat', 'Ginny'), ('Dog', 'Balto'), ('Dog', 'Clifford');")
        conn.execute("UPDATE pets SET age = 15 WHERE name = 'RaceTrack';")

        self.check(
            conn,
            "SELECT species, *, pets.name FROM pets WHERE age > 3 ORDER BY pets.name;",
            [('Dog', 'Ebony', 'Dog', 17, 'Ebony'),
             ('Ferret', 'RaceTrack', 'Ferret', 15, 'RaceTrack'),
                ('Dog', 'Zoe', 'Dog', 7, 'Zoe')]
        )

        conn.execute(
            "INSERT INTO owners VALUES ('Josh', 29, 10), ('Emily', 27, 2), ('Zach', 25, 4), ('Doug', 34, 5);")
        conn.execute("DELETE FROM owners WHERE name = 'Doug';")

        self.check(
            conn,
            "SELECT owners.* FROM owners ORDER BY id;",
            [('Emily', 27, 2), ('Zach', 25, 4), ('Josh', 29, 10)]
        )

        conn.execute("CREATE TABLE ownership (name TEXT, id INTEGER);")
        conn.execute(
            "INSERT INTO ownership VALUES ('RaceTrack', 10), ('Ginny', 2), ('Ghost', 2), ('Zoe', 4);")

        self.check(
            conn,
            "SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE pets.age IS NULL ORDER BY pets.name;",
            [('Balto', None, None), ('Clifford', None, None), ('Ginny', None, 2)]
        )

    def test_connection_interface(self):
        conn = connect("test.db", timeout=0.1, isolation_level=None)
        conn.execute(
            "CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
        conn.execute(
            "INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")

        self.check(
            conn,
            "SELECT * FROM pets ORDER BY pets.name;",
            [('Ebony', 'Dog', 17),
             ('Ghost', 'Ferret', 2),
                ('RaceTrack', 'Ferret', 3),
                ('Zoe', 'Dog', 7)]
        )

    def test_multiple_connections(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute(
            "CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")

        self.check(
            conn_2,
            "SELECT * FROM colors ORDER BY r;",
            [(1, 2, 3)]
        )

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

    def test_multiple_connections_no_transactions(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute(
            "CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")

        self.check(
            conn_2,
            "SELECT * FROM colors ORDER BY r;",
            [(1, 2, 3)]
        )

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)

        self.check(
            conn_3,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

    def test_multiple_connections_later_changes(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute(
            "CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")
        conn_2.execute("INSERT INTO colors VALUES (4, 5, 6);")

        self.check(
            conn_2,
            "SELECT * FROM colors ORDER BY r;",
            [(1, 2, 3), (4, 5, 6)]
        )

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('Josh', 2.4)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam', 4.0);")
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)

        self.check(
            conn_3,
            "SELECT * FROM students ORDER BY name;",
            [('Cam', 4.0), ('Josh', 2.4)]
        )

    def test_create_table_bare(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")

        with self.assertRaises(Exception):
            conn_1.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("CREATE TABLE other (name TEXT);")
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE other (name TEXT);")

        conn_2.execute("CREATE TABLE more (name TEXT);")

    def test_create_table_not_exists(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("CREATE TABLE IF NOT EXISTS students (name TEXT);")

        with self.assertRaises(Exception):
            conn_1.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("CREATE TABLE IF NOT EXISTS other (name TEXT);")
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE other (name TEXT);")

        conn_2.execute("CREATE TABLE more (name TEXT);")

    def test_drop_table_bare(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_1.execute("DROP TABLE students;")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")

    def test_drop_table_if_exists(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")
        conn_1.execute("DROP TABLE IF EXISTS students;")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")

    def test_drop_table_removes_rows(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("DROP TABLE IF EXISTS students;")
        conn_1.execute("CREATE TABLE students (name TEXT);")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            []
        )

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute("INSERT INTO students VALUES ('Zizhen');")
        conn_2.execute("CREATE TABLE IF NOT EXISTS students (name TEXT);")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('Zizhen',)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('Cam',), ('Zizhen',)]
        )

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")

    def test_transaction_syntax(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2.execute("BEGIN TRANSACTION;")
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_2.execute("BEGIN TRANSACTION;")

        conn_2.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_2.execute("COMMIT TRANSACTION;")

    def test_transaction_dml(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        conn_1.execute("BEGIN TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("COMMIT TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

    def test_multiple_transactions_dml(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("BEGIN TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("COMMIT TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

    def test_multiple_transactions_dml_2(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("BEGIN TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_2.execute("BEGIN TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Zizhen');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_2.execute("COMMIT TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("COMMIT TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",), ("Zizhen",)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",), ("Zizhen",)]
        )

    def test_transaction_lock_interference(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("BEGIN TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_2.execute("BEGIN TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Zizhen');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        # Can't commit conn_1 because conn_2 holds a shared lock.
        with self.assertRaises(Exception):
            conn_1.execute("COMMIT TRANSACTION;")

    def test_transaction_lock_interference_2(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("BEGIN TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Cam');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [("Cam",), ("Josh",)]
        )

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_2.execute("BEGIN TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        conn_1.execute("INSERT INTO students VALUES ('Zizhen');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [("Josh",)]
        )

        # Can't perform a write because conn_1 holds a reserved lock.
        with self.assertRaises(Exception):
            conn_2.execute("INSERT INTO students VALUES ('Emily');")

    def test_transaction_mode_syntax(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN DEFERRED TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN IMMEDIATE TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN EXCLUSIVE TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

    def test_transaction_mode_exclusive(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("BEGIN TRANSACTION;")
        conn_2.execute("BEGIN EXCLUSIVE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_3.execute("BEGIN EXCLUSIVE TRANSACTION;")

        conn_3.execute("BEGIN TRANSACTION;")
        conn_2.execute("COMMIT TRANSACTION;")
        conn_4.execute("BEGIN EXCLUSIVE TRANSACTION;")

    def test_transaction_mode_exclusive_dml(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_2.execute("BEGIN EXCLUSIVE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Josh');")

        conn_3.execute("BEGIN TRANSACTION;")
        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Josh');")

        conn_2.execute("INSERT INTO students VALUES ('Josh');")
        with self.assertRaises(Exception):
            conn_4.execute("SELECT * FROM students ORDER BY name;")
            conn_2.execute("COMMIT TRANSACTION;")
            conn_5.execute("INSERT INTO students VALUES ('Josh');")

    def test_transaction_modes_defferred_immediate(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("BEGIN TRANSACTION;")
        conn_2.execute("BEGIN DEFERRED TRANSACTION;")
        conn_3.execute("BEGIN IMMEDIATE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_2.execute("UPDATE students SET name = 'Josh';")
            conn_3.execute("UPDATE students SET name = 'Josh';")

        with self.assertRaises(Exception):
            conn_4.execute("BEGIN IMMEDIATE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_5.execute("BEGIN EXCLUSIVE TRANSACTION;")

    def test_transaction_mode_more(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_2.execute("BEGIN EXCLUSIVE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Zizhen');")

        conn_3.execute("BEGIN TRANSACTION;")
        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Zizhen');")

        conn_2.execute("INSERT INTO students VALUES ('Zizhen');")
        with self.assertRaises(Exception):
            conn_4.execute("SELECT * FROM students ORDER BY name;")

        conn_2.execute("COMMIT TRANSACTION;")

        self.check(
            conn_5,
            "SELECT * FROM students ORDER BY name;",
            [('Zizhen',)]
        )

        conn_5.execute("INSERT INTO students VALUES ('Josh');")

        self.check(
            conn_5,
            "SELECT * FROM students ORDER BY name;",
            [('Josh',), ('Zizhen',)]
        )

    def test_rollback_syntax(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("ROLLBACK TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("ROLLBACK TRANSACTION;")

        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("ROLLBACK TRANSACTION;")

    def test_rollback_undo(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('a');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('a',)]
        )

        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("INSERT INTO students VALUES ('b');")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('a',), ('b',)]
        )

        conn_1.execute("ROLLBACK TRANSACTION;")

        self.check(
            conn_1,
            "SELECT * FROM students ORDER BY name;",
            [('a',)]
        )

        conn_2.execute("BEGIN TRANSACTION;")
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("INSERT INTO students VALUES ('c');")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('a',)]
        )

        conn_2.execute("ROLLBACK TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

        self.check(
            conn_2,
            "SELECT * FROM students ORDER BY name;",
            [('a',), ('c',)]
        )

    def test_multiple_tables(self):
        conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
        conn_5 = connect("test.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT, id INTEGER);")
        conn_2.execute(
            "CREATE TABLE grades (grade INTEGER, name TEXT, student_id INTEGER);")

        conn_3.execute(
            "INSERT INTO students (id, name) VALUES (42, 'Josh'), (7, 'Cam');")
        conn_2.execute(
            "INSERT INTO grades VALUES (99, 'CSE480', 42), (80, 'CSE450', 42), (70, 'CSE480', 9);")

        conn_2.execute("BEGIN DEFERRED TRANSACTION;")
        conn_1.execute("BEGIN IMMEDIATE TRANSACTION;")
        conn_1.execute("INSERT INTO grades VALUES (10, 'CSE231', 1);")

        self.check(
            conn_2,
            "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
        )

        self.check(
            conn_1,
            "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(10, 'CSE231', None),
             (80, 'CSE450', 'Josh'),
             (70, 'CSE480', None),
             (99, 'CSE480', 'Josh')]
        )

        conn_2.execute("COMMIT TRANSACTION;")

        self.check(
            conn_2,
            "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
        )

        with self.assertRaises(Exception):
            conn_3.execute("INSERT INTO students VALUES ('Zach', 732);")

        conn_1.execute("ROLLBACK TRANSACTION;")

        self.check(
            conn_1,
            "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
        )

        conn_1.execute("DROP TABLE IF EXISTS other;")
        conn_3.execute("INSERT INTO students VALUES ('Zach', 732);")

        self.check(
            conn_4,
            "SELECT name FROM students WHERE name > 'A' ORDER BY name;",
            [('Cam',), ('Josh',), ('Zach',)]
        )


if __name__ == "__main__":
    unittest.main()
