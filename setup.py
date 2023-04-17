# coding: utf-8
 
from setuptools import setup, find_packages
 
setup(
    name='sapp',
    version='0.3.1',
    description='Command helper for slurm system. Act as if you are on compute node.',
    url="https://github.com/why-in-Shanghaitech/sapp",
    author='Haoyi Wu',
    author_email='wuhy1@shanghaitech.edu.cn',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    license='MIT',
    entry_points={
        'console_scripts': [
            'sapp=sapp:main',
            'spython=sapp:spython',
            'spython3=sapp:spython3',
        ]
    },
    classifiers = [
        'Programming Language :: Python :: 3 :: Only',
        "Operating System :: OS Independent",
    ],
)
