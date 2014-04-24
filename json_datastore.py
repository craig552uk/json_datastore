# -*- coding: utf-8 -*-
#
# Description:
#
# A simple local NoSQL storage system
# Basically just a wrapper for one big JSON file
#
#
# Example:
#
#   # Create datastore
#   DB = JsonDatastore('datastore.db')
#
#   # Save data
#   data = {'name':'Craig', 'age':31}
#   data = DB.save(type'person', data=data)
#
#   # Update data
#   data['location'] = 'Leicester, UK'
#   data = DB.save(type'person', data=data)
#
#
# Licence:
#
# Copyright (c) 2014 Craig Russell <craig@craig-russell.co.uk>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import json
import time
import hashlib
import random


class JsonDatastore():

    def __init__(self, db_file_path):
        """Create a new NoSqlite DB"""
        self.db_file_path = db_file_path

    def _db_write(self, data):
        """Write data to DB"""
        db = open(self.db_file_path, 'w')
        db.write(json.dumps(data))
        db.close()

    def _db_read(self):
        """Read the contents of the DB"""
        try:
            db = open(self.db_file_path, 'r')
            data = json.loads(db.read())
            db.close()
            return data
        except IOError:
            return {}
        except ValueError:
            return {}

    def _genrate_id(self):
        """Generate a unique id from the current time and a random number"""
        epoch = time.time()
        rand  = random.randint(0,99999)
        return hashlib.sha1("%s-%.6f" % (rand, epoch)).hexdigest()

    def _get_timestamp(self):
        """Return the UTC timestamp"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())

    def save(self, type="", data={}):
        """Save a structured item to DB"""
        db = self._db_read()

        # Create a type store if we need one
        if not type in db.keys():
            db[type] = {}

        # Create an id if we need one
        if not '_id' in data.keys():
            id = self._genrate_id()
            data['_id'] = id
            data['_created'] = self._get_timestamp()

        # Save the data object in the DB
        data['_updated'] = self._get_timestamp()
        db[type][data['_id']] = data
        self._db_write(db)

        # Return the saved object
        return data

    def load(self, type, id):
        """Load an item from dB"""
        try:
            db = self._db_read()
            return db[type][id]

        except KeyError as e:
            raise KeyError("No '%s' with id '%s'" % (type, id))

    def delete_doc(self, type, id):
        """Delete an item from DB"""
        try:
            db = self._db_read()
            del(db[type][id])
            self._db_write(db)

        except KeyError as e:
            raise KeyError("No '%s' with id '%s'" % (type, id))

    def delete_all_docs(self, type):
        """Delete all items of a type"""
        for id in self.list_docs(type):
            self.delete_doc(type, id)

    def delete_type(self, type):
        """Delete a type from the DB"""
        try:
            db = self._db_read()
            del(db[type])
            self._db_write(db)

        except KeyError as e:
            raise KeyError("No document of type '%s' in DB" % type)

    def list_docs(self, type):
        """List all ids for this type"""
        try:
            db = self._db_read()
            return db[type].keys()

        except KeyError as e:
            raise KeyError("No document of type '%s' in DB" % type)

    def list_types(self):
        db = self._db_read()
        return db.keys()



if __name__ == '__main__':

    # Run unit tests 

    import unittest
    import os
    import tempfile

    class TestJsonDatastore(unittest.TestCase):

        def setUp(self):
            self.tmp_file = tempfile.mkstemp()[1]
            self.DB = JsonDatastore(self.tmp_file)

        def tearDown(self):
            os.remove(self.tmp_file)

        def test_save(self):
            # Save new data
            data = self.DB.save(type='person', data={'name':'Craig', 'age':31})
            self.assertTrue('_id' in data)
            self.assertTrue('_created' in data)
            self.assertTrue('_updated' in data)

            # Update
            data['location'] = "Leicester"
            data = self.DB.save(type='person', data=data)
            self.assertTrue("Leicester" == data['location'])

        def test_load(self):
            data_1 = self.DB.save(type='person', data={'name':'Craig', 'age':31})
            data_2 = self.DB.load(type='person', id=data_1['_id'])
            for key in data_1.keys():
                self.assertTrue(data_1[key] == data_2[key])

        def test_delete_doc(self):
            data = self.DB.save(type='person', data={})
            self.DB.delete_doc(type='person', id=data['_id'])
            with self.assertRaises(KeyError):
                self.DB.load(type='person', id=data['_id'])

        def test_delete_all_docs(self):
            data_1 = self.DB.save(type='person', data={})
            data_2 = self.DB.save(type='person', data={})
            data_3 = self.DB.save(type='person', data={})
            data_4 = self.DB.save(type='person', data={})

            self.DB.delete_all_docs(type='person')
            with self.assertRaises(KeyError):
                self.DB.load(type='person', id=data_1['_id'])

            with self.assertRaises(KeyError):
                self.DB.load(type='person', id=data_2['_id'])

            with self.assertRaises(KeyError):
                self.DB.load(type='person', id=data_3['_id'])

            with self.assertRaises(KeyError):
                self.DB.load(type='person', id=data_4['_id'])

        def test_delete_type(self):
            self.DB.save(type='person',  data={})
            self.DB.save(type='animal',  data={})
            self.DB.save(type='country', data={})
            self.DB.save(type='food',    data={})

            self.DB.delete_type('animal')
            type_list = self.DB.list_types()
            self.assertTrue('animal' not in type_list)

        def test_list_types(self):
            self.DB.save(type='person',  data={})
            self.DB.save(type='animal',  data={})
            self.DB.save(type='country', data={})
            self.DB.save(type='food',    data={})

            type_list = self.DB.list_types()
            self.assertTrue(isinstance(type_list, list))
            self.assertEqual(len(type_list), 4)

        def test_list_docs(self):
            self.DB.save(type='person', data={})
            self.DB.save(type='person', data={})
            self.DB.save(type='person', data={})
            self.DB.save(type='person', data={})

            id_list = self.DB.list_docs(type='person')
            self.assertTrue(isinstance(id_list, list))
            self.assertEqual(len(id_list), 4)

    # Run Tests
    unittest.main(verbosity=2)
