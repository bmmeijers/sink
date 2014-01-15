import warnings
warnings.warn("deprecated", DeprecationWarning)

from sink.backends.postgis import Loader

loader = Loader()
loader.bulkload()