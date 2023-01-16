import setuptools

requires = [line.strip() for line in open("requirements.txt").readlines()]
dev_requires = [line.strip() for line in open("devel-requirements.txt").readlines()]


setuptools.setup(
    name="shared_lib",
    version="0.0.1",
    description="Shared library",
    packages=setuptools.find_packages(),
    python_requires='>=3.8',
    zip_safe=False,
    install_requires=requires,
    extras_require={
        "develop": dev_requires
    }
)