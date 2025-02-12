'''
Created on Nov 16, 2011

@author: martijn
'''
__version__ = '0.1.0'
__author__ = 'Martijn Meijers'

from simplegeom.wkb import dumps as as_wkb
from json import dumps as as_json
from .common import Phase

spatial_types = ('point', 'linestring', 'polygon', 'multipolygonz', 'polygonz', 'box2d', )
numeric_types = ('integer', 'bigint', 'numeric', 'float', 'float8')
string_types = ('varchar', )
boolean_types = ('boolean',)
date_types = ('timestamp', 'date', 'time', )
object_types = ('json', )

def loads(layer, limit = None, filter = None):
    
    raise NotImplementedError('to be updated')
    
    from connection.stateful import irecordset
    from simplegeom.postgis import register
    register()
    # Once we have defined a schema / layer definition, it would be 
    # fun to be able to load it
    fields = ",".join(layer.schema.names)
    sql = """
    SELECT
        {0}
    FROM
        {1}
    """.format(fields, layer.name)
    # TODO: implement bbox filter on filter here
    # in case of multiple geometry columns 
    # we can "OR" the filter
    # or we make it more explicit by a different way of methods
    if filter is not None:
        geom_fields = []
        for i, tp in enumerate(layer.schema.types):
            if tp in spatial_types:
                geom_fields.append(layer.schema.names[i])
        for field in geom_fields:
            print(field)
    if limit is not None:
        sql += "LIMIT {0}".format(limit)
    for item in irecordset(sql):
        layer.append(*item)


def dump_schema(layer, stream): #schema, table_name, srid):
    schema = layer.schema
    table_name = layer.name
    srid = layer.srid
    #
    sql = """--
-- Created with sink
-- PostGIS backend v.{0}
-- {1}
--
""".format(__version__, __author__)
    #
    geom_fields = []
    for i, tp in enumerate(schema.types):
        if tp in spatial_types:
            geom_fields.append((i, schema.names[i])) # idx + name    
    # -- drop what is there (raises error / notice if table not there)
    sql += "\nBEGIN;\n"
    for i, field_nm in geom_fields:
        sql += "--COMMIT;BEGIN;\n"
        sql += "--SELECT DropGeometryColumn('{0}', '{1}');\n".format(table_name, field_nm)
        sql += "--COMMIT;BEGIN;\n"
    sql += "DROP TABLE IF EXISTS {0} CASCADE;\n".format(table_name)
    sql += "\nCOMMIT;\n"
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
    sql += ");\n" #WITH (OIDS=TRUE);\n"
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
        dim = 2
        if tp == 'MULTIPOLYGONZ':
            dim = 3
        elif tp == 'POLYGONZ':
            dim = 3
        sql += "SELECT AddGeometryColumn('{0}', '{1}', '{2}', '{3}', {4});\n".format(table_name, field_nm.lower(), srid, tp, dim)
    sql += "COMMIT;\n"
    stream.write(sql)
    stream.flush()


def dump_indices(layer, stream, table_space = "pg_default"):
    stream.write("\nBEGIN;\n")
    for index in layer.schema.indices:
        field_names = [field.name for field in index.fields]
        index_nm = "{0}__".format(layer.name)
        index_nm += "__".join(field_names)        
        index_nm += "__idx"
#        print index, [field.name for field in index.fields]
        if index.primary_key:
            # ALTER TABLE distributors ADD PRIMARY KEY (dist_id);
            #assert len(index.fields) == 1, "index is primary key, so exactly one field required"
            #sql = "ALTER TABLE {0} ADD PRIMARY KEY ({1});\n".format(layer.name, ", ".join(field_names))
            sql = "ALTER TABLE {0} ADD CONSTRAINT {0}_pkey PRIMARY KEY ({1}) USING INDEX TABLESPACE {2};\n".format(layer.name, ", ".join(field_names), table_space)
            stream.write(sql)
            stream.write("\nCOMMIT;\nBEGIN;\n")
        else:
            
            #    CREATE INDEX boekie_centroid_idx ON boekie USING GIST 
            #    ("centroid" gist_geometry_ops) TABLESPACE indx;
            
            assert len(set([field.type for field in index.fields])) == 1, "only same type of fields currently allowed in one index"
            method = "btree" #btree, hash, gist, and gin
            opclass = ""
            for field in index.fields:
                if field.type in spatial_types:
                    method = "gist"
                    # opclass = "gist_geometry_ops"
                    # TODO: opclass has changed in postgis 2
                    # to also support nd indexing, therefore gist_geometry_ops
                    # does not exist any more
                    # @see: http://trac.osgeo.org/postgis/ticket/1287     
                break
            sql = "CREATE INDEX "
            sql += "{0} ".format(index_nm)
            sql += "ON {0} ".format(layer.name.lower())
            sql += "USING {0} ".format(method)
            sql += '("'
            sql += '", "'.join([field.name.lower() for field in index.fields])
            sql += '" {0}'.format(opclass)
            sql += ') '
            sql += 'TABLESPACE "{0}"'.format(table_space)
            sql += ";\n"
            stream.write(sql)
        if index.cluster:
            #CLUSTER indexname ON tablename
            sql = "CLUSTER {0} ON {1};\n".format(index_nm, layer.name)
            stream.write(sql)
    stream.write("COMMIT;\n")
    stream.flush()
    return


def dump_statistics(layer, stream):
    sql = """\nVACUUM ANALYZE {0};\n""".format(layer.name)
    stream.write(sql)
    stream.flush()
    return

def dump_truncate(layer, stream):
    sql = """\nTRUNCATE {0};\n""".format(layer.name)
    stream.write(sql)
    stream.flush()
    return

def dump_drop(layer, stream):
    sql = """\nDROP TABLE {0};\n""".format(layer.name)
    stream.write(sql)
    stream.flush()
    return

def dump_line(layer, feature, stream):
    sql = "INSERT INTO {0} (".format(layer.name)
    defs = []    
    for name in layer.schema.names:
        field_def = '"{0}"'.format( name.lower() )
        defs.append(field_def)
    sql += ", ".join(defs)
    sql += ") VALUES ("
    for i, tp in enumerate(layer.schema.types):
        # no quotes around numeric types, for the rest they will have quotes
        if feature[i] is not None and tp not in numeric_types:
            sql += "'"
        if feature[i] is None: 
#            if tp not in spatial_types:
            sql += "NULL"
#            else:
#                sql += '"{0}"'.format(feature[i])
        else:
            if tp in spatial_types:
                try:
                    assert feature[i].srid == layer.srid, "{0} != {1}".format(feature[i].srid, layer.srid)
                except:
                    if feature[i]:
                        feature[i].srid = layer.srid
                    raise
                if tp == 'box2d':
                    sql += "{0}".format(as_wkb(feature[i].polygon))
                else:
                    sql += "{0}".format(as_wkb(feature[i]))
            elif tp in numeric_types:
                sql += "{0}".format(feature[i])
            elif tp in object_types:
                sql += "{0}".format(as_json(feature[i], allow_nan=False))
            elif tp in boolean_types:
                if feature[i]:
                    sql += "TRUE"
                else:
                    sql += "FALSE"
            elif tp in string_types:
                # single quotes are double, so escaped
                s = feature[i]
                if s is not None:
                    sql += '{0}'.format(s.replace("'", "''"))
            else:
                sql += '{0}'.format(feature[i])
        if feature[i] is not None and tp not in numeric_types:
            sql += "'"
        if i != len(layer.schema.types) - 1:
            sql += ","
    sql += ");\n"
    stream.write(sql)
    stream.flush()

def dump_pre_data(layer, stream):
    # dump_pre_data
    stream.write("\nBEGIN;\n")
    stream.flush()
#     sql = "\nBEGIN;\nCOPY {0} (".format(layer.name)
#     defs = []    
#     for name in layer.schema.names:
#         field_def = '"{0}"'.format( name.lower() )
#         defs.append(field_def)
#     sql += ", ".join(defs)
#     sql += """) FROM STDIN NULL AS 'NULL' CSV QUOTE '"';\n"""
#     stream.write(sql)
#     stream.flush()

def dump_post_data(layer, stream):
    # dump_post_data
    pass
#     stream.write("\.\n\nCOMMIT;\n")
#     stream.flush()
    stream.write("\nCOMMIT;\n")
    stream.flush()
    
def dump_data(layer, stream):
    dump_pre_data(layer, stream)
    for feature in layer.features:
        dump_line(layer, feature, stream)
    dump_post_data(layer, stream)
    return

def dump(layer, stream, phase = Phase.ALL):
    """Dumps a string representation of ``layer'' to buffer like object ``stream''
    
    ``what'' specifies what should be dumped
    """
    if phase & Phase.SCHEMA:
        dump_schema(layer, stream)
    if phase & Phase.DATA:
        dump_data(layer, stream)
    if phase & Phase.INDICES:
        dump_indices(layer, stream)
    if phase & Phase.STATISTICS:
        dump_statistics(layer, stream)

def dumps(layer, phase = Phase.ALL):
    """Returns a string representation of ``layer''
    """
    try:
        from io import StringIO
    except ImportError:
        from io import StringIO
    stream = StringIO()
    dump(layer, stream, phase)
    ret = stream.getvalue()
    stream.close()
    
    return ret

#class StreamingLoader(object):
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

