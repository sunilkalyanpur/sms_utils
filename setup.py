from setuptools import setup

requires = ['requests==2.13.0',
            'boto==2.45.0',
            'SQLAlchemy==1.1.3',
            'flatdict==1.2.0',
            'docopt==0.6.2']

packages = ['sms_utils']

setup(
    name='sms_utils',
    version='0.1',
    url='',
    license='',
    author='',
    author_email='',
    description='Generic Python Utils',
    packages=packages,
    include_package_data=True,
    zip_safe=False,
    install_requires=requires
)
