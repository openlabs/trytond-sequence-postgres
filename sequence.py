"""
    Postgres Sequences

    :copyright: (c) 2011 by Openlabs Technologies & Consulting (P) Ltd..
    :license: GPLv3, see LICENSE for more details.
"""
from trytond.model import ModelView, ModelSQL, fields
from trytond.transaction import Transaction
from trytond.config import CONFIG


class Sequence(ModelSQL, ModelView):
    "Postgres Sequence"
    _name = 'ir.sequence'
    
    def __init__(self):
        if CONFIG.options['db_type'] == 'postgresql':
            postgresql_type = ('postgres_seq', 'Postgres Native Sequence')
            if postgresql_type not in self.type.selection:
                self.type.selection.append(postgresql_type)
        super(Sequence, self).__init__()
        
    def create(self, values):
        """Create the postgres sequence after creation of ir.sequence if the
        type is postrges"""
        id = super(Sequence, self).create(values)
        if values.get('type') == 'postgres_seq':
            self.create_sequence(id)
        return id

    def write(self, ids, values):
        """Write to the postgres sequence after the editing
        """
        ids = [ids] if isinstance(ids, (long, int)) else ids
        for id in ids:
            type_before_write = self.browse(id).type
            rv = super(Sequence, self).write(id, values)
            if type_before_write != 'postgres_seq':
                if values.get('type') != 'postgres_seq':
                    return rv
                self.create_sequence(id)
                return rv
            else:
                if values.get('type') != 'postgres_seq':
                    self.drop_sequence(id)
                    return rv
                self.alter_sequence(id)
        return rv

    def delete(self, ids):
        """Delete the sequence if there is one in postgres"""
        # TODO: DROP the sequence if it exists
        return super(Sequence, self).delete(ids)

    def create_sequence(self, id):
        """CREATE the sequence in database

        :param id: Id of the ir.sequence for which a postgres sequence is to 
            be created
        :type id: int, long
        """
        # The name of the sequence in the database
        sequence = self.browse(id)
        
        with Transaction().new_cursor() as transaction:
            transaction.cursor.execute("""CREATE SEQUENCE ir_sequence_%s 
                INCREMENT BY %s START WITH %s""", (id, 
                    sequence.number_increment, sequence.number_next,))
            transaction.cursor.commit()

        return True

    def alter_sequence(self, id):
        """ALTER the current sequence"""
        sequence = self.browse(id)
        
        with Transaction().new_cursor() as transaction:
            transaction.cursor.execute("""ALTER SEQUENCE ir_sequence_%s 
            INCREMENT BY %s RESTART WITH %s""", (id, sequence.number_increment, 
                sequence.number_next))
            transaction.cursor.commit()

        return True

    def drop_sequence(self, id):
        """DROP the current sequence"""
        sequence = self.browse(id)
        
        with Transaction().new_cursor() as transaction:
            transaction.cursor.execute("""DROP SEQUENCE ir_sequence_%s""", 
                (id,))
            transaction.cursor.commit()

        return True
        
    def _get_sequence(self, sequence):
        """If the sequence type is default pass it on to super function else
        call the select sequence.

        :param domain: This may be the id of the sequence or a domain.
        """
        if sequence.type == 'postgres_seq':
            with Transaction().set_user(0):
                Transaction().cursor.execute("SELECT nextval('ir_sequence_%s')", 
                    (sequence.id,))
                next_id = Transaction().cursor.fetchone()
                return '%%0%sd' % sequence.padding % next_id
        else:
            return super(Sequence, self)._get_sequence(sequence)

Sequence()
