import sys

from setuptools import setup, find_packages

requirements = ['elasticsearch>=1.0.0,<1.1.0', 'tqdm==4.19.5']

if sys.version_info[:2] < (2, 7):
    requirements.append('argparse')
    requirements.append('ordereddict')

setup(
    name='elasticbackup2',
    version='0.4.1',
    packages=find_packages(),

    # metadata for upload to PyPI
    author='Nick Stenning',
    author_email='nick@whiteink.com',
    url='https://github.com/nickstenning/elasticbackup',
    description=('elasticbackup: tools to backup and restore ElasticSearch '
                 'indices'),
    license='MIT',
    keywords='sysadmin elasticsearch backup restore',

    install_requires=requirements,

    entry_points={
        'console_scripts': [
            'elasticbackup2 = elasticbackup.backup:main',
            'elasticrestore2 = elasticbackup.restore:main'
        ]
    }
)
