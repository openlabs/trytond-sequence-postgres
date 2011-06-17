# -*- coding: utf-8 -*-
"""
    test_sequence_postgres

    Test both the sequences on how they work

    :copyright: (c) 2011 by Openlabs Technologies & Consulting (P) Limited
    :license: GPLv3, see LICENSE for more details.
"""

import sys, os
DIR = os.path.abspath(os.path.normpath(os.path.join(__file__,
    '..', '..', '..', '..', '..', 'trytond')))
if os.path.isdir(DIR):
    sys.path.insert(0, os.path.dirname(DIR))
    
import time
import threading
import unittest2 as unittest
from Queue import Queue

from trytond.config import CONFIG
CONFIG['db_type'] = 'postgresql'
CONFIG['db_host'] = 'localhost'
CONFIG['db_port'] = 5432
CONFIG['db_user'] = 'tryton20'
CONFIG['db_password'] = 'tryton'
from trytond.backend.postgresql import Database
import trytond.tests.test_tryton
from trytond.tests.test_tryton import DB_NAME
trytond.tests.test_tryton.DB = Database(DB_NAME)
from trytond.pool import Pool
Pool.test = True
trytond.tests.test_tryton.POOL = Pool(DB_NAME)
from trytond.tests.test_tryton import POOL, USER, CONTEXT, test_view
from trytond.transaction import Transaction


def track_time(func):
    """A decorator to track the time before and after a function and print
    the time elapsed
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        print 'Total time taken: ', time.time() - start_time
    return wrapper


@track_time
def get_id_single_txn(sequence_id, repeat=1000, queue=None):
    """The redundant process of getting the IDS is separated into a function
    so that we dont repeat code.

    All the get_ids is done in a single transaction

    :param sequence_id: ID of the sequence
    :param repeat: No of times the iterator must run
    :param queue: If this is a multithread implementation of the test then
        the regular assert will not work. Hence the id which was returned must
        be pushed into the queue
    """
    sequence_obj = POOL.get('ir.sequence')

    with Transaction().start(DB_NAME, 0, CONTEXT) as txn:
        for expected_id in xrange(1, repeat+1):
            id = sequence_obj.get_id(sequence_id)
            if queue is None:
                # Normal single thread test
                assert int(id) == expected_id
            else:
                # Threaded test where the assert will not work since multiple
                # threads are trying to make the call
                queue.put(int(id))
        txn.cursor.commit()


@track_time
def get_id_separate_txn(sequence_id, repeat=1000, queue=None):
    """The redundant process of getting the IDS is separated into a function
    so that we dont repeat code.

    However each get_id happens on a separate transaction

    :param sequence_id: ID of the sequence
    :param repeat: No of times the iterator must run
    :param queue: If this is a multithread implementation of the test then
        the regular assert will not work. Hence the id which was returned must
        be pushed into the queue

    """
    sequence_obj = POOL.get('ir.sequence')

    for expected_id in xrange(1, repeat+1):
        with Transaction().start(DB_NAME, 0, CONTEXT) as txn:
            id = sequence_obj.get_id(sequence_id)
            if queue is None:
                assert int(id) == expected_id
            else:
                queue.put(int(id))
            txn.cursor.commit()


@track_time
def get_id_separate_txn_code(sequence_code, repeat=1000, queue=None):
    """The redundant process of getting the IDS is separated into a function
    so that we dont repeat code.

    However each get_id happens on a separate transaction

    :param sequence_code: code of the sequence
    :param repeat: No of times the iterator must run
    :param queue: If this is a multithread implementation of the test then
        the regular assert will not work. Hence the id which was returned must
        be pushed into the queue

    """
    sequence_obj = POOL.get('ir.sequence')

    for expected_id in xrange(1, repeat+1):
        with Transaction().start(DB_NAME, 0, CONTEXT) as txn:
            id = sequence_obj.get(sequence_code)
            if queue is None:
                assert int(id) == expected_id
            else:
                queue.put(int(id))
            txn.cursor.commit()


class TestSequencePostgres(unittest.TestCase):
    "Test the cases of the sequence"
        
    def setUp(self):
        trytond.tests.test_tryton.install_module('sequence_postgres')
        self.sequence_obj = POOL.get('ir.sequence')
        self.sequence_type_obj = POOL.get('ir.sequence.type')

    def test_0010_default_sequence(self):
        """Test if the default sequence works like before and called once in 
        every transaction"""
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type',
                'code': 'test.sequence.type.def'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 0010',
                'code': sequence_type.code}) # Values for sequence

            # Step 2: Execute on separately committed transactions
            transaction.cursor.commit()
        get_id_separate_txn(sequence_id)

    def test_0015_default_sequence_using_code(self):
        """Test if the default sequence works like before and called once in 
        every transaction but using a code"""
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.def.code'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            self.sequence_obj.create({
                'name': 'Test Using Code Sequence 0015',
                'code': sequence_type.code})
            transaction.cursor.commit()
        get_id_separate_txn_code(sequence_type.code)

    def test_0020_default_sequence_single_txn(self):
        """Test if the default sequence works like before for a single txn"""
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.def.single'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 0020',
                'code': sequence_type.code}) # Values for sequence
            transaction.cursor.commit()

        # Step 2: The transaction being managed manually (commit after for lp)
        # causes the record of ir_sequence to be accessed and modified several
        # times in the same transaction. There should not be problem here since
        # there is only 1 transaction and its only that transaction which keeps
        # changing values.
        get_id_single_txn(sequence_id)

    @unittest.expectedFailure
    def test_0030_default_sequence_multi_txn(self):
        """Test if the default sequence works like before for multiple transa-
        ctions trying to acquire get_id. This is similar to the previous test
        case but simulating pmultiple transaction trying to do it at the same
        time.
        """
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.def.multi'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 0030',
                'code': sequence_type.code}) # Values for sequence
            transaction.cursor.commit()

        # Step 2: same as case 2 but there will be two transactions trying to 
        # do the same thing at the same time.
        queue = Queue()
        threads = []
        threads.append(
            threading.Thread(
                target = get_id_separate_txn, 
                args = (sequence_id, 500, queue)
                ))
        threads.append(
            threading.Thread(
                target = get_id_separate_txn,
                args = (sequence_id, 500, queue)
                ))
        start_time = time.time()
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        print "Real end time for two threads", time.time() - start_time

        # Ensure that thousand results exist
        self.assertEqual(len(queue.queue), 1000)

        # Now ensure that the queue has no duplicates. The core of the queue is
        # a `collections.deque` - an iterable of which a `set` can be made. If 
        # the length of the set (which has no duplicate elements) is equal to 
        # the length of the queue then there are no duplciates
        self.assertEqual(len(set(queue.queue)), len(queue.queue))

    def test_0110_postgres_sequence(self):
        """Test if the postgres sequence works"""
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.pg'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 3',
                'code': sequence_type.code,
                'type': 'postgres_seq'}) # Values for sequence
            transaction.cursor.commit()

        # Step 2: Call 1000 times. _make_partial_call makes it 1000 separate 
        # auto committed transactions
        get_id_single_txn(sequence_id)

    def test_0120_postgres_sequence_single_txn(self):
        """Test if the postgres sequence works for a single txn"""
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.pg.single'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 4',
                'code': sequence_type.code,
                'type': 'postgres_seq'}) # Values for sequence
            transaction.cursor.commit()

        # Step 2: Call 1000 times. The transaction being managed manually 
        # causes the record of ir_sequence to be accessed and modified several
        # times in the same transaction. 
        get_id_single_txn(sequence_id)

    def test_0130_postgres_sequence_multi_txn(self):
        """Test if the postgres sequence works for multiple transa-
        ctions trying to acquire get_id. This is similar to the previous test
        case but simulating pmultiple transaction trying to do it at the same
        time.
        """
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Code',
                'code': 'test.sequence.type.pg.multi'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 0030',
                'code': sequence_type.code,
                'type': 'postgres_seq'}) # Values for sequence
            transaction.cursor.commit()

        # Step 2: same as case 2 but there will be two transactions trying to 
        # do the same thing at the same time.
        queue = Queue()
        threads = []
        threads.append(
            threading.Thread(
                target = get_id_separate_txn, 
                args = (sequence_id, 500, queue)
                ))
        threads.append(
            threading.Thread(
                target = get_id_separate_txn,
                args = (sequence_id, 500, queue)
                ))
        start_time = time.time()
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        print "Real end time for two threads", time.time() - start_time

        # Ensure that thousand results exist
        self.assertEqual(len(queue.queue), 1000)

        # Now ensure that the queue has no duplicates. The core of the queue is
        # a `collections.deque` - an iterable of which a `set` can be made. If 
        # the length of the set (which has no duplicate elements) is equal to 
        # the length of the queue then there are no duplciates
        self.assertEqual(len(set(queue.queue)), len(queue.queue))
        
    def test_0200_toggle_type(self):
        """Toggle the type of sequence from postgres to default to postgres
        should not break the module
        """
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction:
            # Step 0: Create a new sequence type
            sequence_type_id = self.sequence_type_obj.create({
                'name': 'Test Sequence Type Toggle',
                'code': 'test.sequence.type.toggle'
                })
            sequence_type = self.sequence_type_obj.browse(sequence_type_id)
            # Step 1: Create a new sequence
            sequence_id = self.sequence_obj.create({
                'name': 'Test Sequence 0200',
                'code': sequence_type.code,
                'type': 'postgres_seq'}) # Values for sequence
            #Start toggle
            transaction.cursor.commit()
        with Transaction().start(DB_NAME, 0, CONTEXT) as transaction2:
            self.sequence_obj.write(sequence_id, {'type': 'incremental'})
            self.sequence_obj.write(sequence_id, {'type': 'postgres_seq'})
            transaction2.cursor.commit()
            

def suite():
    "Sequence Postgres test suite"
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(
        TestSequencePostgres
        )
    )
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
