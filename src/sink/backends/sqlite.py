'''
Created on Dec 24, 2011

@author: martijn
'''
__version__ = '0.0.0'
__author__ = 'Martijn Meijers'

from brep.io import as_hexewkb
from .common import SCHEMA, DATA, INDICES, STATISTICS, ALL
import shlex, subprocess
import os, tempfile

spatial_types = ('point', 'linestring', 'polygon', 'box2d', )
numeric_types = ('integer', 'bigint', 'numeric', 'double', 'float', )
string_types =  ('varchar', )

raise NotImplementedError('this is very much not there, just a copy of the postgis backend')

def loads(file):
    raise NotImplementedError('not yet there')

def dump_schema(layer, fh): #schema, table_name, srid):
    schema = layer.schema
    table_name = layer.name
    srid = layer.srid
    #
    sql = """--
-- Created with sink
-- SpatiaLite backend v.{0}
-- {1}
--
""".format(__version__, __author__)
    #
    geom_fields = []
    for i, tp in enumerate(schema.types):
        if tp in spatial_types:
            geom_fields.append((i, schema.names[i])) # idx + name    
    # -- drop what is there (raises error / notice if table not there)
    for i, field_nm in geom_fields:
        sql += "SELECT DropGeometryColumn('{0}', '{1}');\n".format(table_name, field_nm)
    sql += "DROP TABLE IF EXISTS {0};\n".format(table_name)
    sql += "\nBEGIN;\n"
    # -- create table
    sql += "CREATE TABLE {0} (".format(table_name)
    #
    defs = []
    for tp, name in zip(schema.types, schema.names):
        if tp not in spatial_types:
            field_def = '{0} {1}'.format(name.lower(), tp)
            defs.append(field_def)
    sql += ", ".join(defs)
    sql += ");\n"
#    sql += "WITH (OIDS=TRUE);\n"
    # -- add geometry type fields
    for i, field_nm in geom_fields:
        tp = schema.types[i].upper()
        if tp == 'BOX2D':
            tp = 'POLYGON'
#        print schema.types[i].upper()
#        dim = schema.dimensions[i]
#        if dim == 0:
#            tp = "POINT"
#        elif dim == 1:
#            tp = "LINESTRING"
#        elif dim == 2:
#            tp = "POLYGON"
#        else:
#            raise ValueError("Unknown dimension ({0}) given for geometry".format(dim))
        sql += "SELECT AddGeometryColumn('{0}', '{1}', {2}, '{3}', 2);\n".format(table_name, field_nm.lower(), srid, tp)
    sql += "COMMIT;\n"
    fh.write(sql)


def dump_indices(layer, fh, table_space = "indx"):
    fh.write("\nBEGIN;\n")
    for index in layer.schema.indices:
#        print index, [field.name for field in index.fields]
        if index.primary_key:
            # ALTER TABLE distributors ADD PRIMARY KEY (dist_id);
            #assert len(index.fields) == 1, "index is primary key, so exactly one field required"
            flds = []
            for field in index.fields:
                flds.append(field.name)
            sql = "ALTER TABLE {0} ADD PRIMARY KEY ({1});\n".format(layer.name, ", ".join(flds))
            fh.write(sql)
        else:
            #    CREATE INDEX boekie_centroid_idx ON boekie USING GIST 
            #    ("centroid" gist_geometry_ops) TABLESPACE indx;
            field_names = [field.name for field in index.fields]
            assert len(set([field.type for field in index.fields])) == 1, "only same type of fields currently allowed in one index"
            method = "btree" #btree, hash, gist, and gin
            opclass = ""
            
            # spatial index apparently is created
            # as virtual tables, with this type of statement
            # SELECT CreateSpatialIndex('local_councils', 'geometry');
            
            for field in index.fields:
                if field.type in spatial_types:
                    method = "gist"
                    opclass = "gist_geometry_ops"        
                break
            sql = "CREATE INDEX "
            sql += "{0}__".format(layer.name)
            sql += "__".join(field_names)        
            sql += "__idx ON {0} ".format(layer.name)
            sql += "USING {0} ".format(method)
            sql += '("'
            sql += '", "'.join([field.name for field in index.fields])
            sql += '" {0}'.format(opclass)
            sql += ') '
            # sql += "TABLESPACE {0}".format(table_space)
            sql += ";\n"
            fh.write(sql)
    fh.write("COMMIT;\n")
    return


def dump_statistics(layer, fh):
    sql = """\nVACUUM ANALYZE {0};\n""".format(layer.name)
    fh.write(sql)
    return

def dump_truncate(layer, fh):
    sql = """\nTRUNCATE {0};\n""".format(layer.name)
    fh.write(sql)
    return

def dump_drop(layer, fh):
    sql = """\nDROP TABLE {0};\n""".format(layer.name)
    fh.write(sql)
    return

def dump_line(layer, feature, fh):
    sql = ""
    for i, tp in enumerate(layer.schema.types):
        if tp in spatial_types:
            if not feature[i] is None:
                if tp == 'box2d':
                    sql += "{0}".format(as_hexewkb(feature[i].polygon, layer.srid))
                else:
                    sql += "{0}".format(as_hexewkb(feature[i], layer.srid))
            elif feature[i] is None:
                sql += "NULL"
            else:
                sql += "'{0}'".format(feature[i])
        elif tp in numeric_types:
            if feature[i] is None:
                sql += "NULL"
            else:
                sql += "{0}".format(feature[i])
        elif feature[i] is None:
            sql += "NULL"
        else:
            sql += "'{0}'".format(feature[i])
        if i != len(layer.schema.types) - 1:
            sql += ","
    sql += "\n"
    fh.write(sql)

def dump_pre_data(layer, fh):
    # dump_pre_data
    sql = "\nBEGIN;\nCOPY {0} (".format(layer.name)
    defs = []    
    for name in layer.schema.names:
        field_def = '"{0}"'.format( name )
        defs.append(field_def)
    sql += ", ".join(defs)
    sql += """) FROM STDIN NULL AS 'NULL' CSV QUOTE '"';\n"""
    fh.write(sql)

def dump_post_data(layer, fh):
    # dump_post_data
    fh.write("\.\n\nCOMMIT;\n")
    
def dump_data(layer, fh):
    dump_pre_data(layer, fh)
    for feature in layer.features:
        dump_line(layer, feature, fh)
    dump_post_data(layer, fh)
    return

def dump(layer, fh, what = ALL):
    """Dumps a string representation of ``layer'' to buffer like object ``fh''
    
    ``what'' specifies what should be dumped
    """
    if what & SCHEMA:
        dump_schema(layer, fh)
    if what & DATA:
        dump_data(layer, fh)
    if what & INDICES:
        dump_indices(layer, fh)
    if what & STATISTICS:
        dump_statistics(layer, fh)

def dumps(layer, what = ALL):
    """Returns a string representation of ``layer''
    """
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    fh = StringIO()
    dump(layer, fh, what)
    ret = fh.getvalue()
    fh.close()
    return ret

#class Loader(object):
#
#    def __init__(self):
#        self._tmpdir = tempfile.mkdtemp()
#        self._filename = os.path.join(self._tmpdir, 'myfifo')
#        try:
#            os.mkfifo(self._filename)
#            print self._filename
#        except OSError, e:
#            print "Failed to create FIFO: %s" % e
#        else:
#            print "opening fifo"
#            self._fifo = open(self._filename, 'r')
#            print "opened fifo for reading"
#
#    def bulkload(self):
#        """
#        bulkload `filename' into database
#        """
#        # write stuff here...
#        from connect import auth_params
#        auth = auth_params()
#        psql_env = dict(PGPASSWORD='{0[password]}'.format(auth))
#        cmd = shlex.split('nohup psql -U {0[username]} -d {0[database]} -h {0[host]} -f {1} &'.format(auth, self._filename))
#        proc = subprocess.Popen(cmd, shell=True, env=psql_env)
#        proc.communicate()
#
#    def close(self):
#        self._fifo.close()
#        os.remove(self._filename)
#        os.rmdir(self._tmpdir)

