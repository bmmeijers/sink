#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages, Extension
import os

def get_version():
    """
    Gets the version number. Pulls it from the source files rather than
    duplicating it.
    """
    fn = os.path.join(os.path.dirname(__file__), 'src', 'sink', '__init__.py')
    try:
        lines = open(fn, 'r').readlines()
    except IOError:
        raise RuntimeError("Could not determine version number"
                           "(%s not there)" % (fn))
    version = None
    for l in lines:
        # include the ' =' as __version__ might be a part of __all__
        if l.startswith('__version__ =', ):
            version = eval(l[13:])
            break
    if version is None:
        raise RuntimeError("Could not determine version number: "
                           "'__version__ =' string not found")
    return version

PACKAGES = find_packages('src')
EXT_MODULES = []
SCRIPTS = [] 
REQUIREMENTS = []
DATA_FILES = []

setup(
    name = "sink",
    version = get_version(),
    packages = PACKAGES,
    package_dir = {"": "src"},
    author = "Martijn Meijers",
    author_email = "b.m.meijers@tudelft.nl",
    description = "A lightweight abstraction for Table Oriented Output",
    url = "https://bitbucket.org/bmmeijers/sink/",
    license = "MIT license",
    ext_modules = EXT_MODULES,
    data_files = DATA_FILES,
    zip_safe = False,
    scripts = SCRIPTS,
    install_requires = REQUIREMENTS,
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
