# Setup for machines that only need to queue jobs.
import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='UCLDC Harvester',
    version='0.8.1',
    py_modules=['harvester.config', ],
    include_package_data=True,
    license='BSD License - see LICENSE file',
    description='Harvester installed for queuing jobs for the UCLDC project',
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
    install_requires=[
        'requests==2.11.1',
        'Logbook==0.6.0',
        'redis==2.10.1',
        'rq',
        ],
    test_suite='test',
    tests_require=['mock>=1.0.1', 'httpretty==0.8.3', ],
)
