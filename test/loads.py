import warnings
warnings.warn("deprecated", DeprecationWarning)
from sink import Field, Schema, Index, Layer, Phase, loads

# generic attributes
gid = Field("gid", "numeric")
object_begin = Field("object_begin_tijd", "timestamp")
versie_begin = Field("versie_begin_tijd", "timestamp")
status = Field("status", "varchar")
brontype = Field("brontype", "varchar")
bronbeschrijving = Field("bronbeschrijving", "varchar")
bronactualiteit = Field("bronactualiteit", "date")
bronnauwkeurigheid = Field("bronnauwkeurigheid", "numeric")
dimensie = Field("dimensie", "varchar")

# waterdelen
type_water = Field("type_water", "varchar")
breedteklasse = Field("breedteklasse", "varchar")
hoofdafwatering = Field("hoofdafwatering", "varchar")
functie = Field("functie", "varchar")
voorkomen_water = Field("voorkomen_water", "varchar")
stroomrichting = Field("stroomrichting", "varchar")
sluisnaam = Field("sluisnaam", "varchar")

# terrein
type_landgebruik = Field("type_landgebruik", "varchar")
voorkomen = Field("voorkomen", "varchar")
naam = Field("naam", "varchar")

# wegdelen
type_weg = Field("type_weg", "varchar")
type_infrastructuur = Field("type_infrastructuur", "varchar")
hoofdverkeersgebruik = Field("hoofdverkeersgebruik", "varchar")
fysiek_voorkomen = Field("fysiek_voorkomen", "varchar")
verhardingsbreedteklasse = Field("verhardingsbreedteklasse", "varchar")
gescheiden_rijbaan = Field("gescheiden_rijbaan", "varchar")
verhardingstype = Field("verhardingstype", "varchar")
aantal_rijstroken = Field("aantal_rijstroken", "numeric")
straatnaam = Field("straatnaam", "varchar")
hoogteniveau = Field("hoogteniveau", "numeric")
a_wegnummer = Field("a_wegnummer", "varchar")
n_wegnummer = Field("n_wegnummer", "varchar")
s_wegnummer = Field("s_wegnummer", "varchar")
afritnummer = Field("afritnummer", "varchar")
knooppuntnaam = Field("knooppuntnaam", "varchar")
brugnaam = Field("brugnaam", "varchar")
tunnelnaam = Field("tunnelnaam", "varchar")

# geometrie
geometrie_punt = Field("geometrie_punt", "point")
geometrie_lijn = Field("geometrie_lijn", "linestring")
geometrie_vlak = Field("geometrie_vlak", "polygon")
hart_punt = Field("hart_punt", "point")
hart_lijn = Field("hart_lijn", "linestring")



pkey_idx = Index(fields = [gid], primary_key = False)

geom_punt_idx = Index([geometrie_punt])
geom_lijn_idx = Index([geometrie_lijn])
geom_vlak_idx = Index([geometrie_vlak], cluster = True)

hart_punt_idx = Index([hart_punt])
hart_lijn_idx = Index([hart_lijn])


schema = Schema([#
                 gid, 
                 object_begin, versie_begin, status,
                 brontype, bronbeschrijving, bronactualiteit, bronnauwkeurigheid, dimensie,
                 # attributes
                 type_weg, type_infrastructuur, hoofdverkeersgebruik, fysiek_voorkomen,
                 verhardingsbreedteklasse, gescheiden_rijbaan,
                 verhardingstype, aantal_rijstroken, straatnaam, hoogteniveau,
                 a_wegnummer, n_wegnummer, s_wegnummer, afritnummer,
                 knooppuntnaam, brugnaam, tunnelnaam,
                 # geometrie
                 geometrie_punt, geometrie_lijn, geometrie_vlak,
                 hart_punt, hart_lijn,
                 ],
                [pkey_idx, 
                 geom_punt_idx, geom_lijn_idx, geom_vlak_idx,
                 hart_punt_idx, hart_lijn_idx])

layer = Layer(schema, "top10nl_wegdeel", srid=28992)
loads(layer, limit = 10, filter = [(0,0), (10,10)])
print len(layer.features), "retrieved"

L = len(schema.fields)
for j, feature in enumerate(layer.features):
    print ""
    print "feature", j
    for i in xrange(L):
        print layer.schema.names[i], ">>", feature[i]