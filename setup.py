import pathlib
import re
import setuptools

text = (pathlib.Path(__file__).parent / 'zerofun/__init__.py').read_text()
version = re.search(r"__version__ = '(.*)'", text).group(1)

setuptools.setup(
    name='zerofun',
    version=version,
    author='Danijar Hafner',
    author_email='mail@danijar.com',
    description='Remote function calls for array data using ZMQ',
    url='http://github.com/danijar/zerofun',
    long_description=pathlib.Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    install_requires=['numpy', 'msgpack', 'pyzmq', 'cloudpickle'],
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
)
