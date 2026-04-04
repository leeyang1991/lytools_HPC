
from setuptools import setup

def get_version():
    init_f = './lytools_HPC/__init__.py'
    with open(init_f) as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[-1].strip().strip("'")
version = get_version()

long_description = open('README.md').read()
setup(
    name='lytools_HPC',
    version=version,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Yang Li',
    author_email='leeyang1991@gmail.com',
    packages=['lytools_HPC'],
    url='https://github.com/leeyang1991/lytools_HPC',
    python_requires='>=3',
    install_requires=[
    'submitit',
    'redis',
    'pathos',
    'rich',
    'requests',
    'packaging',
    ],
)
