'''
Created on Nov 16, 2011

@author: martijn
'''

__version__ = '0.0.2'
__author__ = 'Martijn Meijers'

#Since all table/index names must be stored in the data dictionary and only 30 
#characters are allocated for the storage, 
#the maximum name size is 30 characters

from simplegeom.wkb import dumps
import hashlib

spatial_types = ('point', 'linestring', 'polygon', 'box2d', )
numeric_types = ('integer', 'bigint', 'numeric', 'float', )
string_types =  ('varchar', )
boolean_types = ('boolean',)
date_types = ('timestamp', 'date', 'time', )

GEOM_SCHEMA = "mdsys"
#raise NotImplementedError('not yet there')

def loads(man0):
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

#SDO_geometry (2003, null, null,
#          SDO_elem_info_array (1,1003,3),
#          SDO_ordinate_array (-109,37,-102,40));

def dump_schema(layer, stream0): #schema, table_name, srid):
    schema = layer.schema
    table_name = layer.name
    srid = layer.srid
    #
    sql = """--
-- Created with sink
-- Oracle backend v.{0}
-- {1}
--

SET FEEDBACK OFF;
SET DEFINE OFF; 
""".format(__version__, __author__)
    #
#    geom_fields = []
#    for i, tp in enumerate(schema.types):
#        if tp in spatial_types:
#            geom_fields.append((i, schema.names[i])) # idx + name    
    # -- drop what is there (raises error / notice if table not there)
#    for i, field_nm in geom_fields:
#        sql += "SELECT DropGeometryColumn('{0}', '{1}');\n".format(table_name, field_nm)
    sql += """
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE {0}';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

""".format(table_name)
    
    sql += "\n--START TRANSACTION;\n"
    # -- create table
    sql += "CREATE TABLE {0} (".format(table_name)
    #
    defs = []
    for tp, name in zip(schema.types, schema.names):
        if tp not in spatial_types and tp not in string_types:
            # -- add non-geometry type fields
            field_def = '{0} {1}'.format(name.lower(), tp)
            defs.append(field_def)
        elif tp in string_types:
            # set to max size for varchar (required in definition)
            field_def = '{0} {1}(4000)'.format(name.lower(), tp) 
            defs.append(field_def)
        elif tp in spatial_types:
            # -- add geometry type fields
            field_def = '{0} {1}'.format(name.lower(), "{}.sdo_geometry".format(GEOM_SCHEMA))
            defs.append(field_def)
    sql += ", ".join(defs)
    sql += ");\n"
    # -- add geometry type fields
#    for i, field_nm in geom_fields:
#        tp = schema.types[i].upper()
#        if tp == 'BOX2D':
#            tp = 'POLYGON'
#        sql += "SELECT AddGeometryColumn('{0}', '{1}', '{2}', '{3}', 2);\n".format(table_name, field_nm.lower(), srid, tp)
    sql += "COMMIT;\n"
    stream0.write(sql)


def dump_indices(layer, stream0, table_space = "users"):
    stream0.write("\n--START TRANSACTION;\n")
    
    for tp, field_nm in zip(layer.schema.types, layer.schema.names):
        if tp in spatial_types:
            stream0.write("""
DELETE FROM user_sdo_geom_metadata WHERE table_name = '{0}' AND column_name = '{1}';
INSERT INTO user_sdo_geom_metadata (diminfo, table_name, column_name, srid)
VALUES (
( SELECT MDSYS.SDO_DIM_ARRAY( 
                        MDSYS.SDO_DIM_ELEMENT('X', minx, maxx, 0.0001), 
                        MDSYS.SDO_DIM_ELEMENT('Y', miny, maxy, 0.0001)) as diminfo 
             FROM ( SELECT 0 as minx, --TRUNC( MIN( v.x ) - 1,0) as minx,
                           1 as maxx, --ROUND( MAX( v.x ) + 1,0) as maxx,
                           0 as miny, --TRUNC( MIN( v.y ) - 1,0) as miny,
                           1 as maxy --ROUND( MAX( v.y ) + 1,0) as maxy
                      FROM (SELECT SDO_AGGR_MBR(a.{1}) as mbr
                              FROM {0} a) b,
                                   TABLE(mdsys.sdo_util.getvertices(b.mbr)) v
                   )
         ),
         '{0}',
         '{1}',
         {2}
);
COMMIT;
""".format(layer.name.upper(), field_nm.upper(), layer.srid)
    )
    
    for index in layer.schema.indices:
#        print index, [field.name for field in index.fields]
        if index.primary_key:
            # ALTER TABLE distributors ADD PRIMARY KEY (dist_id);
            #assert len(index.fields) == 1, "index is primary key, so exactly one field required"
            flds = []
            for field in index.fields:
                flds.append(field.name)
            indx_nm = "{0}__{1}".format(layer.name, "__".join(flds))
            md5_indx_nm = hashlib.md5(indx_nm).hexdigest()[:26]
            sql = "ALTER TABLE {0} ADD CONSTRAINT pky_{1} PRIMARY KEY ({2});\n".format(layer.name, md5_indx_nm, ", ".join(flds))
#            ALTER TABLE table_name
#            add CONSTRAINT constraint_name PRIMARY KEY (column1, column2, ... column_n);
            
            stream0.write(sql)
        else:
            #    CREATE INDEX boekie_centroid_idx ON boekie USING GIST 
            #    ("centroid" gist_geometry_ops) TABLESPACE indx;
            field_names = [field.name for field in index.fields]
            assert len(set([field.type for field in index.fields])) == 1, "only same type of fields currently allowed in one index"
#            method = "btree" #btree, hash, gist, and gin
#            opclass = ""
#            for field in index.fields:
#                if field.type in spatial_types:
#                    method = "gist"
#                    opclass = "gist_geometry_ops"        
#                break
            indx_nm = "{0}__{1}".format(layer.name, "__".join(field_names))
            md5_indx_nm = hashlib.md5(indx_nm).hexdigest()[:26]

            sql = "CREATE INDEX "
            sql += "idx_{0}".format(md5_indx_nm)
            sql += " ON {0} ".format(layer.name)
#            sql += "USING {0} ".format(method)
            sql += '('
            sql += '", "'.join([field.name for field in index.fields])
#            sql += '" {0}'.format(opclass)
            sql += ') '

            for field in index.fields:
                if field.type in spatial_types:
                    sql += "\nINDEXTYPE IS MDSYS.SPATIAL_INDEX\n"
#                    method = "gist"
#                    opclass = "gist_geometry_ops"        
                    break
            else:
                sql += "TABLESPACE {0}".format(table_space)
            sql += ";\n"
            stream0.write(sql)
#            CREATE INDEX [schema.]<index_name> ON [schema.]<tableName> (column)
#             INDEXTYPE IS MDSYS.SPATIAL_INDEX
#             [PARAMETERS ('index_params [physical_storage_params]' )]
#             [{ NOPARALLEL | PARALLEL [ integer ] }];

#CREATE INDEX emp_ename ON emp(ename)
#      TABLESPACE users
#      STORAGE (INITIAL 20K
#      NEXT 20k
#      PCTINCREASE 75);
      
    stream0.write("COMMIT;\n")
    return


def dump_statistics(layer, stream0):
    sql = """\nANALYZE TABLE {0} COMPUTE STATISTICS;\n""".format(layer.name)
    stream0.write(sql)
    return

def dump_truncate(layer, stream0):
    sql = """\nTRUNCATE {0};\n""".format(layer.name)
    stream0.write(sql)
    return

def dump_drop(layer, stream0):
    sql = """\nDROP TABLE {0};\n""".format(layer.name)
    stream0.write(sql)
    return

def dump_line(layer, feature, stream):
    # TODO, rewrite here if feature[i] is None, it is always the same -> NULL
    # this should be handled earlier (instead of all ifs separately)
    sql = "INSERT INTO {} ({}) VALUES (".format(layer.name, ", ".join([name for name in layer.schema.names]))
    for i, tp in enumerate(layer.schema.types):
        sql += "\n"
        if tp in spatial_types:
            if not feature[i] is None:
#                CREATE TYPE sdo_geometry AS OBJECT (
#                 SDO_GTYPE NUMBER, 
#                 SDO_SRID NUMBER,
#                 SDO_POINT SDO_POINT_TYPE,
#                 SDO_ELEM_INFO SDO_ELEM_INFO_ARRAY,
#                 SDO_ORDINATES SDO_ORDINATE_ARRAY);
                if tp == 'box2d':
                    # optimized rectangle
                    sql += """
MDSYS.SDO_GEOMETRY(
    2003,
    {1},
    NULL,
    SDO_ELEM_INFO_ARRAY(1,1003,3),
    SDO_ORDINATE_ARRAY({0.xmin}, {0.ymin}, {0.xmax}, {0.ymax})
)""".format(feature[i], layer.srid)
                elif tp == 'point':
                    sql += """
MDSYS.SDO_GEOMETRY(
    2001,
    {1},
    MDSYS.SDO_POINT_TYPE(
    {0[0]},{0[1]},null),
    null,
    null
)""".format(feature[i], layer.srid)
                elif tp == 'linestring':
                    sql += """
MDSYS.SDO_GEOMETRY(
    2002,
    {1},
    null, 
    MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),
    MDSYS.SDO_ORDINATE_ARRAY({0})
)""".format(",".join(["{0[0]},{0[1]}".format(pt) for pt in feature[i]]), layer.srid)
                elif tp == 'polygon':
                    
                    elem_infos = []
                    ordinates = []
                    ct = 1
                    for r, ring in enumerate(feature[i]):
                        if r:
                            hole = "2003"
                        else:
                            hole = "1003"
                        ordinates.append(",".join(["{0[0]},{0[1]}".format(pt) for pt in ring]))
                        elem_infos.append(",".join([str(ct), hole, "1"]))
                        ct += 2*len(ring)
                    elem_info = ",\n    ".join(elem_infos)
                    ordinate = ",\n    ".join(ordinates)
                    sql += """
MDSYS.SDO_GEOMETRY(
    2003, 
    {1}, 
    NULL,
    MDSYS.SDO_ELEM_INFO_ARRAY(
        {2}
    ),
    MDSYS.SDO_ORDINATE_ARRAY(
        {3}
    )
)""".format(feature[i], layer.srid, elem_info, ordinate)
                else:
                    raise NotImplementedError('This geometry type {} is not yet implemented'.format(tp))
#                    sql += "{0}".format(as_hexewkb(feature[i], layer.srid))
                    
            elif feature[i] is None:
                sql += "NULL"
            else:
                sql += "'{0}'".format(feature[i])
        elif tp in numeric_types and feature[i] is not None:  
            sql += "{0}".format(feature[i])
        elif tp in date_types and feature[i] is not None:
            if tp == 'timestamp':
                sql += "TIMESTAMP '{0}'".format(feature[i])
            elif tp == 'date':
                sql += "CAST(TIMESTAMP '{0}' AS DATE)".format(feature[i])
            elif tp == 'time':
                sql += "CAST(TIMESTAMP '{0}' AS TIME)".format(feature[i])
        elif tp in string_types and feature[i] is not None:
            sql += "'{0}'".format(str(feature[i]).replace("'", r"''"))
        elif feature[i] is None:
            sql += "NULL"
        else:
            sql += "'{0}'".format(feature[i])
        if i != len(layer.schema.types) - 1:
            sql += ","
    sql += ");\n"
    stream.write(sql)

def dump_pre_data(layer, stream0):
    # dump_pre_data
#    sql = "\n--START TRANSACTION;\nCOPY {0} (".format(layer.name)
#    defs = []    
#    for name in layer.schema.names:
#        field_def = '"{0}"'.format( name )
#        defs.append(field_def)
#    sql += ", ".join(defs)
#    sql += """) FROM STDIN CSV QUOTE '"';\n"""
#    stream0.write(sql)
    pass

def dump_post_data(layer, stream0):
    # dump_post_data
    stream0.write("\n\nCOMMIT;\n")
    
def dump_data(layer, stream0):
    dump_pre_data(layer, stream0)
    for feature in layer.features:
        dump_line(layer, feature, stream0)
    dump_post_data(layer, stream0)
    return

def dump(layer, stream, data = True):
    """Dumps a string representation of ``layer'' to buffer like object ``stream''
    """
    dump_schema(layer, stream)
    if data:
        dump_data(layer, stream)
    dump_indices(layer, stream)
    dump_statistics(layer, stream)

def dumps(layer, data = True):
    """Returns a string representation of ``layer''
    """
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    stream0 = StringIO()
    dump(layer, stream0, data)
    ret = stream0.getvalue()
    stream0.close()
    return ret