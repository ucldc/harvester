import os
from pip import main as pip_main
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='UCLDC Harvester',
    version='0.0',
    packages=['harvester', 'harvester.post_processing' ],
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
        'https://github.com/mredar/md5s3stash/archive/master.zip#egg=md5s3stash',
        'https://github.com/ucldc/pynux/archive/master.zip#egg=pynux',
        'https://pypi.python.org/packages/source/p/pilbox/pilbox-1.0.3.tar.gz#egg=pilbox',
    ],
    install_requires=[
        'Sickle==0.3',
        'argparse==1.2.1',
        'lxml==3.3.5',
        'requests==2.1.0',
        'solrpy==0.9.6',
        'pilbox',
        'wsgiref==0.1.2',
        'Logbook==0.6.0',
        'amara',
        'akara',
        'python-dateutil==2.2',
        'CouchDB==0.9',
        'redis==2.10.1',
        'rq==0.4.6',
        'boto==2.29.1',
        'CouchDB==0.9',
        'md5s3stash',
        'pymarc>=3.0',
        'pynux',
        ],
    test_suite='test',
    tests_require=['mock==1.0.1', 'httpretty==0.8.3', ],
)

pip_main(['install', 'ansible'])
pip_main(['install',
         'git+ssh://git@bitbucket.org/mredar/dpla-ingestion.git@ucldc'])
