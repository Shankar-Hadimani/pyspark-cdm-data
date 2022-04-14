from setuptools import setup

setup(name='pipelines',
      version='0.0.1',
      description='PySpark Application to manage SCD2, SCD1 and Full Refresh loading strategies',
      url='https://mygithub.gsk.com/gsk-tech/PSCTech-Biopharm-CODi-Ingestion',
      author='GSK PSCTech',
      author_email='shankar.hadimani@XXX.com',
      packages=['pipelines', 'pipelines.utils', 'pipelines.jobs', 'dependencies'],
      zip_safe=False)