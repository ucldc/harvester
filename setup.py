import os
import sys
import subprocess
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='UCLDC Harvester',
    version='0.8.1',
    packages=['harvester', 'harvester.fetcher', 'harvester.post_processing'],
    include_package_data=True,
    license='BSD License - see LICENSE file',
    description='harvester code for the UCLDC project',
    long_description=read('README.md'),
    author='Mark Redar',
    author_email='mark.redar@ucop.edu',
    classifiers=[
        'Environment :: Web Environment',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
    dependency_links=[
        'https://github.com/zepheira/amara/archive/master.zip#egg=amara',
        'https://github.com/zepheira/akara/archive/master.zip#egg=akara',
        'https://github.com/ucldc/md5s3stash/archive/7c32a3270198ae9b84f22a4852fe60105f74651b.zip#egg=md5s3stash',
        'https://github.com/ucldc/pynux/archive/master.zip#egg=pynux',
        'https://raw.githubusercontent.com/ucldc/facet_decade/master/facet_decade.py#egg=facet_decade-2.0',
        'https://pypi.python.org/packages/source/p/pilbox/pilbox-1.0.3.tar.gz#egg=pilbox',
         'https://github.com/mredar/redis-collections/archive/master.zip#egg=redis-collections',
        'https://github.com/mredar/nuxeo-calisphere/archive/master.zip#egg=UCLDC-Deep-Harvester',
        'https://github.com/tingletech/mediajson/archive/master.zip#egg=mediajson',
        'https://github.com/mredar/sickle/archive/master.zip#egg=Sickle',
        # 'https://github.com/nvie/rq/archive/4875331b60ddf8ddfe5b374ec75c938eb9749602.zip#egg=rq',
    ],
    install_requires=[
        'Sickle==0.5',
        'argparse==1.2.1',
        'lxml==3.3.5',
        'requests==2.11.1',
        'solrpy==0.9.7',
        'pysolr==3.3.0',
        'pilbox==1.0.3',
        'wsgiref==0.1.2',
        'Logbook==0.6.0',
        'amara==2.0.0',
        'akara==2.0.0a4',
        'python-dateutil==2.2',
        'CouchDB==0.9',
        'redis>=2.10.1',
        'rq==0.13.0',
        'boto==2.49.0',
        'md5s3stash',
        'pymarc==3.0.4',
        'facet_decade==2.0',
        'redis_collections==0.1.7',
        'xmljson==0.2.0',
        'UCLDC-Deep-Harvester',
        'boto3==1.9.160',
        'pynux',
        'mediajson'
        ],
    test_suite='test',
    tests_require=['mock>=1.0.1', 'httpretty==0.9.5', ],
)

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'ansible'])

#pip_main(['install', 'ansible'])
#pip_main(['install',
#'git+https://github.com/ucldc/pynux.git@b539959ac11caa6fec06f59a0b3768d97bec2693'])
###pip_main(['install',
###         'git+ssh://git@bitbucket.org/mredar/dpla-ingestion.git@ucldc'])
