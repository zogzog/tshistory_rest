from setuptools import setup


setup(name='tshistory_rest',
      version='0.1.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr',
      url='https://bitbucket.org/pythonian/tshistory_rest',
      description='timeseries histories http front & python client',

      packages=['tshistory_rest'],
      install_requires=[
          'flask-restplus',
          'tshistory',
          'requests',
          'pytest_sa_pg',
      ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
