from sink import use
use('oracle')
from sink import Field, Schema, Index, Layer, Phase
import sys

##############################################################################
# Here the layer definitions are given for the model 
# to which we load the parsed features
##############################################################################

srid = 28992

gid = Field("gid", "numeric")
# GeoObject
identificatie = Field("identificatie", "varchar")
brontype = Field("brontype", "varchar")
bronbeschrijving = Field("bronbeschrijving", "varchar")
bronactualiteit = Field("bronactualiteit", "date")
bronnauwkeurigheid = Field("bronnauwkeurigheid", "float")
dimensie = Field("dimensie", "varchar")
object_begin_tijd = Field("object_begin_tijd", "timestamp")
versie_begin_tijd = Field("versie_begin_tijd", "timestamp")
object_eind_tijd = Field("object_eind_tijd", "timestamp")
versie_eind_tijd = Field("versie_eind_tijd", "timestamp")
visualisatie_code = Field("visualisatie_code", "numeric")
tdn_code = Field("tdn_code", "numeric")
geo_object = [ gid, identificatie, brontype, bronbeschrijving, bronactualiteit, bronnauwkeurigheid, dimensie, object_begin_tijd, versie_begin_tijd, object_eind_tijd, versie_eind_tijd, visualisatie_code, tdn_code ]

# Wegdeel
aantal_rijstroken = Field("aantal_rijstroken", "numeric")
afritnaam = Field("afritnaam", "varchar")
afritnummer = Field("afritnummer", "varchar")
a_wegnummer = Field("a_wegnummer", "varchar")
brugnaam = Field("brugnaam", "varchar")
e_wegnummer = Field("e_wegnummer", "varchar")
fysiek_voorkomen = Field("fysiek_voorkomen", "varchar")
geometrie_lijn = Field("geometrie_lijn", "linestring")
geometrie_punt = Field("geometrie_punt", "point")
geometrie_vlak = Field("geometrie_vlak", "polygon")
gescheiden_rijbaan = Field("gescheiden_rijbaan", "varchar")
hart_lijn = Field("hart_lijn", "linestring")
hart_punt = Field("hart_punt", "point")
hoofdverkeersgebruik = Field("hoofdverkeersgebruik", "varchar")
hoogteniveau = Field("hoogteniveau", "numeric")
knooppuntnaam = Field("knooppuntnaam", "varchar")
n_wegnummer = Field("n_wegnummer", "varchar")
status = Field("status", "varchar")
straatnaam_fries = Field("straatnaam_fries", "varchar")
straatnaam_nl = Field("straatnaam_nl", "varchar")
s_wegnummer = Field("s_wegnummer", "varchar")
tunnelnaam = Field("tunnelnaam", "varchar")
type_infrastructuur_wegdeel = Field("type_infrastructuur_wegdeel", "varchar")
type_weg = Field("type_weg", "varchar")
verhardingsbreedte = Field("verhardingsbreedte", "float")
verhardingsbreedteklasse = Field("verhardingsbreedteklasse", "varchar")
verhardingstype = Field("verhardingstype", "varchar")
wegdeel = [ aantal_rijstroken, afritnaam, afritnummer, a_wegnummer, brugnaam, e_wegnummer, fysiek_voorkomen, geometrie_lijn, geometrie_punt, geometrie_vlak, gescheiden_rijbaan, hart_lijn, hart_punt, hoofdverkeersgebruik, hoogteniveau, knooppuntnaam, n_wegnummer, status, straatnaam_fries, straatnaam_nl, s_wegnummer, tunnelnaam, type_infrastructuur_wegdeel, type_weg, verhardingsbreedte, verhardingsbreedteklasse, verhardingstype ]

# FunctioneelGebied
label_punt = Field("label_punt", "point")
naam_fries = Field("naam_fries", "varchar")
naam_nl = Field("naam_nl", "varchar")
type_functioneel_gebied = Field("type_functioneel_gebied", "varchar")
functioneel_gebied = [ geometrie_vlak, label_punt, naam_fries, naam_nl, type_functioneel_gebied ]

# Waterdeel
breedte = Field("breedte", "float")
breedteklasse = Field("breedteklasse", "varchar")
functie = Field("functie", "varchar")
hoofdafwatering = Field("hoofdafwatering", "varchar")
scheepslaadvermogen = Field("scheepslaadvermogen", "float")
sluisnaam = Field("sluisnaam", "varchar")
stroomrichting = Field("stroomrichting", "varchar")
type_infrastructuur_waterdeel = Field("type_infrastructuur_waterdeel", "varchar")
type_water = Field("type_water", "varchar")
voorkomen_water = Field("voorkomen_water", "varchar")
waterdeel = [ breedte, breedteklasse, brugnaam, functie, fysiek_voorkomen, geometrie_lijn, geometrie_punt, geometrie_vlak, hoofdafwatering, hoogteniveau, naam_fries, naam_nl, scheepslaadvermogen, sluisnaam, status, stroomrichting, type_infrastructuur_waterdeel, type_water, voorkomen_water ]

# Terrein
type_landgebruik = Field("type_landgebruik", "varchar")
voorkomen = Field("voorkomen", "varchar")
terrein = [ fysiek_voorkomen, geometrie_vlak, hoogteniveau, naam_fries, naam_nl, type_landgebruik, voorkomen ]

# Gebouw
hoogte = Field("hoogte", "float")
hoogteklasse = Field("hoogteklasse", "varchar")
type_gebouw = Field("type_gebouw", "varchar")
gebouw = [ geometrie_vlak, hoogte, hoogteklasse, hoogteniveau, naam_nl, naam_fries, status, type_gebouw ]

# Inrichtingselement
nummer = Field("nummer", "varchar")
type_inrichtingselement = Field("type_inrichtingselement", "varchar")
inrichtingselement = [ geometrie_lijn, geometrie_punt, hoogte, hoogteniveau, naam_nl, nummer, status, type_inrichtingselement, naam_fries ]

# Spoorbaandeel
aantal_sporen = Field("aantal_sporen", "numeric")
baanvaknaam = Field("baanvaknaam", "varchar")
elektrificatie = Field("elektrificatie", "varchar")
spoorbreedte = Field("spoorbreedte", "varchar")
type_infrastructuur_spoorbaandeel = Field("type_infrastructuur_spoorbaandeel", "varchar")
type_spoorbaan = Field("type_spoorbaan", "varchar")
vervoerfunctie = Field("vervoerfunctie", "varchar")
spoorbaandeel = [ aantal_sporen, baanvaknaam, brugnaam, elektrificatie, fysiek_voorkomen, geometrie_lijn, geometrie_punt, hoogteniveau, spoorbreedte, status, tunnelnaam, type_infrastructuur_spoorbaandeel, type_spoorbaan, vervoerfunctie ]

# GeografischGebied
aantal_inwoners = Field("aantal_inwoners", "numeric")
type_geografisch_gebied = Field("type_geografisch_gebied", "varchar")
geografisch_gebied = [ aantal_inwoners, geometrie_vlak, label_punt, naam_nl, type_geografisch_gebied, naam_fries ]

# RegistratiefGebied
type_registratief_gebied = Field("type_registratief_gebied", "varchar")
registratief_gebied = [ geometrie_vlak, label_punt, naam_fries, naam_nl, nummer, type_registratief_gebied ]

# Relief
type_relief = Field("type_relief", "varchar")
relief = [ functie, hoogte, hoogteklasse, hoogteniveau, naam_fries, naam_nl, status, type_relief ]

# HoogteOfDieptePunt
hoogte_of_diepte_punt = [ geometrie_punt ]

# Hoogteverschil
lage_zijde = Field("lage_zijde", "linestring")
hoge_zijde = Field("hoge_zijde", "linestring")
hoogteverschil = [ lage_zijde, hoge_zijde ]

# IsoHoogte
iso_hoogte = [ geometrie_lijn ]

# KadeOfWal
kade_of_wal = [ geometrie_lijn ]

# OverigRelief
overig_relief = [ geometrie_punt, geometrie_lijn ]

# Field lists
wegdeel = geo_object + wegdeel
waterdeel = geo_object + waterdeel
gebouw = geo_object + gebouw
terrein = geo_object + terrein
spoorbaandeel = geo_object + spoorbaandeel
inrichtingselement = geo_object + inrichtingselement
functioneel_gebied = geo_object + functioneel_gebied
geografisch_gebied = geo_object + geografisch_gebied
registratief_gebied = geo_object + registratief_gebied

hoogte_of_diepte_punt = geo_object + relief + hoogte_of_diepte_punt 
hoogteverschil = geo_object + relief + hoogteverschil
iso_hoogte = geo_object + relief + iso_hoogte
kade_of_wal = geo_object + relief + kade_of_wal
overig_relief = geo_object + relief + overig_relief

# Indices
pkey_idx = Index(fields = [gid], primary_key = True)

geom_punt_idx = Index([geometrie_punt])
geom_lijn_idx = Index([geometrie_lijn])
geom_vlak_idx = Index([geometrie_vlak], cluster = True)

hart_punt_idx = Index([hart_punt])
hart_lijn_idx = Index([hart_lijn])

label_punt_idx = Index([label_punt])

hoge_zijde_idx = Index([hoge_zijde])
lage_zijde_idx = Index([lage_zijde])

# schema's
wegdeel_schema = Schema(wegdeel,
[pkey_idx, 
 geom_punt_idx, geom_lijn_idx, geom_vlak_idx,
 hart_punt_idx, hart_lijn_idx])

waterdeel_schema = Schema(waterdeel,
[pkey_idx, 
 geom_punt_idx, geom_lijn_idx, geom_vlak_idx,
 ])

gebouw_schema = Schema(gebouw,
[pkey_idx, 
 geom_vlak_idx,
 ])

terrein_schema = Schema(terrein,
[pkey_idx, 
 geom_vlak_idx,
 ])

spoorbaandeel_schema = Schema(spoorbaandeel,
[pkey_idx, 
 geom_punt_idx, geom_lijn_idx, 
 ])

inrichtingselement_schema = Schema(inrichtingselement,
[pkey_idx, 
 geom_punt_idx, geom_lijn_idx, 
 ])

functioneel_gebied_schema = Schema(functioneel_gebied,
[pkey_idx, 
 label_punt_idx, 
 geom_vlak_idx,
 ])

geografisch_gebied_schema = Schema(geografisch_gebied,
[pkey_idx, 
 label_punt_idx, 
 geom_vlak_idx,
 ])

registratief_gebied_schema = Schema(registratief_gebied,
[pkey_idx, 
 label_punt_idx, 
 geom_vlak_idx,
 ])

hoogte_of_diepte_punt_schema = Schema(hoogte_of_diepte_punt, [pkey_idx, geom_punt_idx,]) 
hoogteverschil_schema = Schema(hoogteverschil, [pkey_idx, lage_zijde_idx, hoge_zijde_idx]) 
iso_hoogte_schema = Schema(iso_hoogte, [pkey_idx, geom_lijn_idx]) 
kade_of_wal_schema = Schema(kade_of_wal, [pkey_idx, geom_lijn_idx]) 
overig_relief_schema = Schema(overig_relief, [pkey_idx, geom_punt_idx, geom_lijn_idx]) 

PREFIX = "top10nl_"
# layers
#def wegdeel(fh, phase = Phase.ALL):
#    return StreamingLayer(wegdeel_schema, PREFIX+"wegdeel", srid = srid, phase = phase, stream = fh)
#
#def waterdeel(fh, phase = Phase.ALL):
#    return StreamingLayer(waterdeel_schema, PREFIX+"waterdeel", srid = srid, phase = phase, stream = fh)
#
#def gebouw(fh, phase = Phase.ALL):
#    return StreamingLayer(gebouw_schema, PREFIX+"gebouw", srid = srid, phase = phase, stream = fh)
#
#def terrein(fh, phase = Phase.ALL):
#    return  StreamingLayer(terrein_schema, PREFIX+"terrein", srid = srid, phase = phase, stream = fh)
#
#def spoorbaandeel(fh, phase = Phase.ALL):
#    return StreamingLayer(spoorbaandeel_schema, PREFIX+"spoorbaandeel", srid = srid, phase = phase, stream = fh)
#
#def inrichtingselement(fh, phase = Phase.ALL):
#    return StreamingLayer(inrichtingselement_schema, PREFIX+"inrichtingselement", srid = srid, phase = phase, stream = fh)
#
#def functioneel_gebied(fh, phase = Phase.ALL):
#    return StreamingLayer(functioneel_gebied_schema, PREFIX+"functioneel_gebied", srid = srid, phase = phase, stream = fh)
#
#def geografisch_gebied(fh, phase = Phase.ALL):
#    return StreamingLayer(geografisch_gebied_schema, PREFIX+"geografisch_gebied", srid = srid, phase = phase, stream = fh)
#
#def registratief_gebied(fh, phase = Phase.ALL):
#    return StreamingLayer(registratief_gebied_schema, PREFIX+"registratief_gebied", srid = srid, phase = phase, stream = fh)
#
#def hoogte_of_diepte_punt(fh, phase = Phase.ALL): 
#    return StreamingLayer(hoogte_of_diepte_punt_schema, PREFIX+"hoogte_of_diepte_punt", srid = srid, phase = phase, stream = fh)
#
#def hoogteverschil(fh, phase = Phase.ALL): 
#    return StreamingLayer(hoogteverschil_schema, PREFIX+"hoogteverschil", srid = srid, phase = phase, stream = fh)
#
#def iso_hoogte(fh, phase = Phase.ALL): 
#    return StreamingLayer(iso_hoogte_schema, PREFIX+"iso_hoogte", srid = srid, phase = phase, stream = fh)
#
#def kade_of_wal(fh, phase = Phase.ALL): 
#    return StreamingLayer(kade_of_wal_schema, PREFIX+"kade_of_wal", srid = srid, phase = phase, stream = fh)
#
#def overig_relief(fh, phase = Phase.ALL): 
#    return StreamingLayer(overig_relief_schema, PREFIX+"overig_relief", srid = srid, phase = phase, stream = fh)




from sink import dumps

from simplegeom.geometry import Polygon, LineString, LinearRing, Point
from datetime import datetime
outer = LinearRing([Point(0,0), Point(10, 0), Point(5,10), Point(0,0)])
inner = LinearRing([Point(1,1), Point(9, 1), Point(5,9), Point(1,1)])

defn = [gid, tdn_code, object_begin_tijd,
        bronactualiteit, 
        brontype,
        geometrie_vlak, geometrie_lijn, geometrie_punt]
lyr = Layer(Schema(defn, []), "wazaa")
lyr.append(10, 1003, datetime.now(),
           datetime.now(), "a beautiful product",
           Polygon(outer, [inner]), LineString([Point(0,0), Point(10, 0), Point(5,10), Point(0,0)]), Point(100,100))

print dumps(lyr)