"""Sink, a lightweight abstraction for Table Oriented Output

Created on Aug 18, 2010

@author: martijn
"""

__version__ = '0.7.0'
__license__ = 'MIT license'
__author__ = 'Martijn Meijers'

import collections
import sys
from backends.common import Phase

import logging
log = logging.getLogger(__name__)


# Probably this is also helpful:
# http://code.google.com/p/web2py/source/browse/gluon/dal.py


# - created, aug 18, 2010, MM
# - update,  dec 13, 2010, MM: added indexing & split dumping of data into parts

# Design considerations / goals:
#
# - light weight, no validation of whatsoever
#   (considered nullables, uniqueness -> this then more looks like a main memory
#    database, then that it is a light weight solution for data storage)
#
# - no support for removals of objects already appended (as this means some sort of indexing to find the objects)
#
# - put less burden on programmer to create output (by redefining layers over and over again)
#
# - should play nicely with shapefiles / PostGIS / Oracle DBMS tables / REDIS in memory store?
#   (in addition to field definitions, preferences for indexing, uniqueness, and so on could be supplied)
#

#def use(name):
#    """This module imports dynamically the requirements.
#    
#    A preference for which backend should be used, could be stored in a module
#    config.py
#    if multiple modules will load dynamically
#    
#    See: 
#    http://effbot.org/pyfaq/how-do-i-share-global-variables-across-modules.htm
#    """
#    backendname = 'sink.backends.{0}'.format(name)
#    mod = __import__(backendname, 
#                     globals=globals(), 
#                     locals=locals(), # not used according to doc 
#                     fromlist='*', #
#                     level=0) # absolute imports
#    # we load these bits
#    requirements = ('spatial_types', 
#                    'numeric_types',
#                    'string_types',
#                    'dump',
#                    'dumps',

#                    'dump_schema',

#                    'dump_pre_data',
#                    'dump_data',
#                    'dump_post_data',

#                    'dump_line',

#                    'dump_indices',

#                    'dump_statistics',)
#    for req in requirements:
#        globals()[req] = getattr(mod, req)
#
#use('postgis')

#from backends.postgis import 
#spatial_types, 
#numeric_types, 
#string_types, \
#dump, dumps, \
#dump_schema, \
#dump_pre_data, dump_line, dump_post_data, dump_data, \
#dump_indices, \
#dump_statistics

def use(name):
    backendname = 'sink.backends.{0}'.format(name)
    mod = __import__(backendname, globals=globals(), locals=locals(), fromlist='*', level=0) #, globals(), locals(), [backendname])
#    method = getattr(mod, 'dump')
#    globals()['dump'] = method
    lst = ['spatial_types', 'numeric_types', 'string_types', 'date_types', 'boolean_types',
            'dump', 'dumps', 'loads',
            'dump_schema', 'dump_drop',
            'dump_pre_data', 'dump_line', 'dump_post_data', 'dump_data',
            'dump_indices',
            'dump_statistics']
    for item in lst:
        importing = getattr(mod, item)
        globals()[item]= importing

use('postgis')


class Schema(object):
    def __init__(self, fields = [], indices = []):
        self.fields = []
        self.indices = []
        for field in fields:
            self.add_field(field)
        for index in indices:
            self.add_index(index)

    def add_field(self, field):
        assert (field.type in spatial_types) or \
                (field.type in numeric_types) or \
                (field.type in string_types) or \
                (field.type in boolean_types) or \
                (field.type in date_types), \
                'Unknown type found: "{0}" for field "{1}"'.format(field.type, 
                                                                   field.name) 
        self.fields.append(field)
    
    def add_index(self, index):
        self.indices.append(index)
    
    @property
    def names(self):
        names = [f.name for f in self.fields]
        return names

    @property
    def types(self):
        types = [f.type for f in self.fields]
        return types

    @property
    def dimensions(self):
        sizes = [f.dimension for f in self.fields]
        return sizes


class Field(object):
    __slots__ = ("name", "type", "dimension", "decimals") #, "nullable") ## (type, dimension, decimal places)
    def __init__(self, name, tp, dimension = None, decimals = None):
        self.name = str(name)
        self.type = str(tp)        # integer, double, string, geometry
        self.dimension = dimension # for geometry, pt = 0, ln = 1, poly = 2, for strings, you can put max length of string
        self.decimals = decimals   # for floats, put precision behind comma

class Layer(object):
    def __init__(self, schema, name, srid = -1, options = None):
        assert len(name) > 0
        self.schema = schema
        self.name = name
        self.srid = srid
        self.options = options
        self.features = []
        self.Feature = collections.namedtuple('Feature', ", ".join(self.schema.names))

    def __len__(self):
        return len(self.features)

    def __iter__(self):
        return iter(self.features)

    def append(self, *attrs):
        """Factory method for features"""
        f = self.Feature._make(attrs)
        self.features.append(f)
        #for i, nullable in enumerate(self.schema.nullables):
        #    if not nullable and f[i] is None:
        #        raise ValueError('Found null value for "{0}"'.format(self.schema.names[i]))
        return f

    def clear(self):
        self.features = []

    def finalize(self):
        pass
    

class StreamingLayer(object):
    def __init__(self, schema, name, srid = -1, stream = None, unbuffered = False, options = None):
        self._count = 0
        self._finalized = False
        assert len(name) > 0
        self.schema = schema
        self.name = name
        self.srid = srid
        self.options = options
        self._unbuffered = unbuffered
        if stream is not None:
            self._stream = stream
        else:
            self._stream = sys.stdout
        self.Feature = collections.namedtuple('Feature', ", ".join(self.schema.names))
        
        # TODO:
        # If we would ever need something that has mutable attributes,
        # this recipe could help 
        # http://code.activestate.com/recipes/576555/

# New API, instead of using Phase?
#
# clear()
# werkt niet:
# init(data=True) # default
# append(...)
# append(...)
# finalize()

    def init(self):
        dump_schema(self, self._stream)
        self._stream.flush()

    def pre_data(self):
        dump_pre_data(self, self._stream)
        self._stream.flush()

    def __len__(self):
        return self._count

    def append(self, *attrs):
        """Factory method for features"""
        if self._finalized:
            raise ValueError('append method called, while stream already finalized')
        f = self.Feature._make(attrs)
        self._count += 1
        dump_line(self, f, self._stream)
        if self._unbuffered:
            self._stream.flush()
        #for i, nullable in enumerate(self.schema.nullables):
        #    if not nullable and f[i] is None:
        #        raise ValueError('Found null value for "{0}"'.format(self.schema.names[i]))
        return f

    def clear(self):
        # TODO: in principle could truncate the stream
        # but that would not work with fifo's no possibility to clear what
        # is written any more...
#        self._stream.seek(0)
#        self._stream.truncate(0)
        raise ValueError('clear method does not work for StreamingLayer')

    def post_data(self):
        dump_post_data(self, self._stream)
        if self._unbuffered:    
            self._stream.flush()

    def finalize(self, table_space = 'indx'):
        log.debug("Indexing tables - tablespace: {}".format(table_space))
        # dump_post_data
        dump_indices(self, self._stream, table_space)
        dump_statistics(self, self._stream)
        self._stream.flush()
#        self._stream.close()
        self._finalized = True
    
    def drop(self):
        log.debug("Dropping table {}".format(self.name))
        dump_drop(self, self._stream)


class Index(object):
    def __init__(self, fields, primary_key = False, cluster = False):
        self.fields = []
        self.primary_key = primary_key
        self.cluster = cluster
        for field in fields:
            self.add_field(field)
    
    def add_field(self, field):
        self.fields.append(field)
