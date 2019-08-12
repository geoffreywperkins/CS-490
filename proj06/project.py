"""
Simple Document Store

Name: Geoffrey Witherington-Perkins
Time To Completion: 2 hrs
Comments: :)

Sources: Python JSON library documentation
"""


import json


class Collection:
    """
    A list of dictionaries (documents) accessible in a DB-like way.
    """

    def __init__(self, documents=None):
        """
        Initialize an empty collection.
        """
        if not documents:
            documents = []
        self.documents = documents

    def insert(self, document):
        """
        Add a new document (a.k.a. python dict) to the collection.
        """
        self.documents.append(document)

    def find_all(self):
        """
        Return list of all docs in database.
        """
        return self.documents

    def delete_all(self):
        """
        Truncate the collection.
        """
        self.documents.clear()

    def check_document(self, document, where_dict):
        for key in where_dict:
            if key not in document:
                return False

            # Check for nested {}, recursive call on the inner dictionaries
            if type(document[key]) == dict:
                if not self.check_document(document[key], where_dict[key]):
                    return False
            elif document[key] != where_dict[key]:
                return False
        return True

    def find_one(self, where_dict):
        """
        Return the first matching doc.
        If none is found, return None.
        """

        for document in self.documents:
            if self.check_document(document, where_dict):
                return document

    def find(self, where_dict):
        """
        Return matching list of matching doc(s).
        """
        matching_list = []
        for document in self.documents:
            if self.check_document(document, where_dict):
                matching_list.append(document)
        print(matching_list)
        return matching_list

    def count(self, where_dict):
        """
        Return the number of matching docs.
        """

        # return len(self.find(where_dict))
        count = 0
        for document in self.documents:
            if self.check_document(document, where_dict):
                count += 1
        return count

    def delete(self, where_dict):
        """
        Delete matching doc(s) from the collection.
        """
        remove_list = self.find(where_dict)

        for document in remove_list:
            self.documents.remove(document)

    def update(self, where_dict, changes_dict):
        """
        Update matching doc(s) with the values provided.
        """
        change_list = self.find(where_dict)
        for document in change_list:
            for key in changes_dict:
                document[key] = changes_dict[key]

    def map_reduce(self, map_function, reduce_function):
        """
        Applies a map_function to each document, collating the results.
        Then applies a reduce function to the set, returning the result.
        """
        map_results = []
        for document in self.documents:
            map_results.append(map_function(document))

        return reduce_function(map_results)


class Database:
    """
    Dictionary-like object containing one or more named collections.
    """

    def __init__(self, filename):
        """
        Initialize the underlying database. If filename contains data, load it.
        """
        self.collections = {}

        try:
            self.file_pointer = open(filename)
            file_contents = json.load(self.file_pointer)

            for collection_name in file_contents:
                self.collections[collection_name] = Collection(file_contents[collection_name])

        except FileNotFoundError:
            self.file_pointer = open(filename, "w")

    def get_collection(self, name):
        """
        Create a collection (if new) in the DB and return it.
        """
        if name not in self.collections:
            self.collections[name] = Collection()
        return self.collections[name]

    def drop_collection(self, name):
        """
        Drop the specified collection from the database.
        """
        if name in self.collections:
            del self.collections[name]

    def get_names_of_collections(self):
        """
        Return a list of the sorted names of the collections in the database.
        """
        return sorted(self.collections.keys())

    def close(self):
        """
        Save and close file.
        """
        file_contents = {}
        for collection_name in self.collections:
            file_contents[collection_name] = self.collections[collection_name].find_all()

        json.dump(file_contents, self.file_pointer)

        self.file_pointer.close()
