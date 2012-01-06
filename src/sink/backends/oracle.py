'''
Created on Nov 16, 2011

@author: martijn
'''

__version__ = '0.0.1'
__author__ = 'Martijn Meijers'

from brep.io import as_hexewkb

spatial_types = ('point', 'linestring', 'polygon', 'box2d', )
numeric_types = ('integer', 'bigint', 'numeric', 'double', 'float', )
string_types =  ('varchar', )

raise NotImplementedError('not yet there')

def loads(file):
    raise NotImplementedError('not yet there')


#create table point_tst (
#  OBJECTID NUMBER(38) NOT NULL UNIQUE,
#  NAME VARCHAR2(32),
#  LON NUMBER,
#  LAT NUMBER,
#  SHAPE MDSYS.SDO_GEOMETRY
#);
#insert into point_tst values (
#  1,
#  'Boulder',
#  -105.2700,
#  40.0150,
#  MDSYS.SDO_GEOMETRY(
#     2001,
#     8307,
#     MDSYS.SDO_POINT_TYPE(-105.2700, 40.0150, NULL),
#     NULL,
#     NULL
#  )
#);
#insert into point_tst values (
#  2,
#  'Denver',
#  -104.9842,
#  39.7392,
#  MDSYS.SDO_GEOMETRY('POINT (-104.9842 39.7392)', 8307)
#);
#
#insert into user_sdo_geom_metadata values (
#  'POINT_TST',
#  'SHAPE',
#  MDSYS.SDO_DIM_ARRAY(
#     MDSYS.SDO_DIM_ELEMENT('LONGITUDE',-180,180,0.05), 
#     MDSYS.SDO_DIM_ELEMENT('LATITUDE',-90,90,0.05)
#  ),
#  8307);
#create index point_tst_spatial_idx on point_tst(SHAPE) 
#indextype is MDSYS.SPATIAL_INDEX parameters ('LAYER_GTYPE=point');

def dump_schema(layer, fh): #schema, table_name, srid):
    schema = layer.schema
    table_name = layer.name
    srid = layer.srid
    #
    sql = "--\n-- Created with sink, Oracle backend v.{0}\n-- {1}\n--\n".format(__version__, __author__)
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
    sql += ") WITH (OIDS=TRUE);\n"
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
        sql += "SELECT AddGeometryColumn('{0}', '{1}', '{2}', '{3}', 2);\n".format(table_name, field_nm.lower(), srid, tp)
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
            sql += "TABLESPACE {0}".format(table_space)
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
    sql += """) FROM STDIN CSV QUOTE '"';\n"""
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

def dump(layer, fh, data = True):
    """Dumps a string representation of ``layer'' to buffer like object ``fh''
    """
    dump_schema(layer, fh)
    if data:
        dump_data(layer, fh)
    dump_indices(layer, fh)
    dump_statistics(layer, fh)

def dumps(layer, data = True):
    """Returns a string representation of ``layer''
    """
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    fh = StringIO()
    dump(layer, fh, data)
    ret = fh.getvalue()
    fh.close()
    return ret