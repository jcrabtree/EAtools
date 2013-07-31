from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='EAtools',
      version='0.1',
      description='A selection of tools used by the Electricity Authority',
      url='https://github.com/ElectricityAuthority/EAtools',
      author='Market Performance team',
      author_email='imm@ea.govt.nz',
      license='https://github.com/ElectricityAuthority/LICENSE',
      packages=['EAtools',
                'EAtools.EAstyles',
                'EAtools.cds_gnash',
                'EAtools.data_warehouse'],
      install_requires=['pandas',],
      zip_safe=False)
