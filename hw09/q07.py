import requests
import re
import xml.etree.ElementTree as ET
import sqlite3

def GrabCSE480Page(url):
    """Solution from HW8
    Args:
        url (str): The second parameter.
    Returns:
        rows: List of tuples of extracted site data.
    """
    response = requests.get(url)
    html = response.text
    html = html.replace("</p>\n                </p>", "</p>")
    root = ET.fromstring(html)
    body = root[1]
    div = body[2]
    row_fluid = div[0]
    span9 = row_fluid[2]
    table = span9[5]
    rows = []
    for row_data in table:
        new_row = []
        for child in row_data:
            new_text = ""
            if child.text:
                new_text += child.text

            for inner_child in child:
                new_text += inner_child.text
                if inner_child.tail:
                    new_text += inner_child.tail
            new_row.append(new_text)
        rows.append(tuple(new_row))
    return rows

def headers_to_sql_columns(column_headers):
    """Converts a tuple of strings to SQL formatted column names of type TEXT.
    E.g.
    input -> ('Week (Date)', 'Link to Videos', 'Project Due Dates', 'Notes')
    output -> Week_Date TEXT, Link_to_Videos TEXT, Project_Due_Dates TEXT, Notes TEXT
    Args:
        column_headers (tuple): Tuple of column names.
    Returns:
        columns: Column names/types cleaned of bad characters and
                 formatted for sqlite table creation
    """
    BAD_CHARS = '()\\\'\"!@#$%^&*()\{\}[]'
    print(column_headers)
    column_list = ['{} TEXT'.format(col.replace(' ','_')) for col in column_headers]
    columns = ', '.join(column_list)
    for char in BAD_CHARS:
        columns = columns.replace(char, '')
    print(columns)
    return columns

"""DO NOT NEED TO MODIFY ABOVE THIS LINE """

def create_cse480_database(table_data):
    """Creates database for cse480 site information
    Args:
        table_data (list): List of tuples. See GrabCSE480Page.
    Returns:
        Connection to the database created.
    """
    # Clean up first tuple to simplify table creation
    headers = table_data.pop(0)
    columns = headers_to_sql_columns(headers)
    # MODIFY BELOW
    # CREATE A DATABASE
    conn = sqlite3.connect('ZeDatabaseOhYah.db')
    # CREATE A TABLE
    conn.execute("CREATE TABLE cse480 (" + columns + ");")
    # INSERT TABLE DATA
    # RETURN CONNECTION TO DATABASE
    return conn