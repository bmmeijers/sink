import warnings
warnings.warn("deprecated", DeprecationWarning)

from sink import Schema, Field, Index, Layer, StreamingLayer

# if you want to switch backend, do it here, 
# i.e. before dump / dumps gets imported...
from sink import use
use('oracle')
#use('postgis')

from sink import dump, dumps
#from sink import SCHEMA, DATA, INDICES, STATISTICS, ALL

def test():
    from brep.geometry import Point, Envelope
    from StringIO import StringIO
    
    gid = Field("edge_id", "numeric")
    left = Field("left_face_id", "numeric")
    right = Field("right_face_id", "numeric")
    start = Field("start_node_id", "numeric")
    end = Field("end_node_id", "numeric")
    point = Field("geometry", "point")
    centroid = Field("centroid", "point")
    envelope = Field("envelope", "box2d")
    
    schema = Schema()
    schema.add_field(gid)
    schema.add_field(left)
    schema.add_field(right)
    schema.add_field(start)
    schema.add_field(end)
    schema.add_field(point)
    schema.add_field(centroid)
    schema.add_field(envelope)
    
    schema.add_index( Index(fields = [gid], primary_key = True) )
    schema.add_index( Index(fields = [left]) )
    schema.add_index( Index(fields = [right]) )
    schema.add_index( Index(fields = [start]) )
    schema.add_index( Index(fields = [end]) )
    schema.add_index( Index(fields = [centroid]) )
    schema.add_index( Index(fields = [point]) )
    
    layer = Layer(schema, "boekie")
    
    layer.append(1, 5, 6, 15, 16, Point(10, 10), Point(10, 10), Envelope(0,0,10, 10))
    layer.append(2, 5, 6, 15, 16, Point(10,15), Point(10, 10), Envelope(0,0,10, 10))

    dumps(layer)

    fh = StringIO()
    dump(layer, fh)
    val = fh.getvalue()
    print val
    fh.close()



def test_stream():
    from simplegeom.geometry import Point, Envelope
    from StringIO import StringIO
    
    gid = Field("edge_id", "numeric")
    left = Field("left_face_id", "numeric")
    right = Field("right_face_id", "numeric")
    start = Field("start_node_id", "numeric")
    end = Field("end_node_id", "numeric")
    point = Field("geometry", "point")
    centroid = Field("centroid", "point")
    envelope = Field("envelope", "box2d")
    
    schema = Schema([], [])
    schema.add_field(gid)
    schema.add_field(left)
    schema.add_field(right)
    schema.add_field(start)
    schema.add_field(end)
    schema.add_field(point)
    schema.add_field(centroid)
    schema.add_field(envelope)
    
    schema.add_index( Index(fields = [gid], primary_key = True) )
    schema.add_index( Index(fields = [left]) )
    schema.add_index( Index(fields = [right]) )
    schema.add_index( Index(fields = [start]) )
    schema.add_index( Index(fields = [end]) )
    schema.add_index( Index(fields = [centroid]) )
    schema.add_index( Index(fields = [point]) )
    
#    fh = open('/tmp/stream', 'w')
    
#     layer = StreamingLayer(schema, "boekie", what = ALL) #stream = fh)
#     layer.append(1, 5, 6, 15, 16, Point(10, 10), Point(10, 10), Envelope(0,0,10, 10))
#     layer.append(2, 5, 6, 15, 16, Point(10,15), Point(10, 10), Envelope(0,0,10, 10))
#     layer.finalize()
#     
#    fh.close()

if __name__ == '__main__':
#    use('postgis')
#    test()
    test_stream()