import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eigensheep",
    version="0.0.2",
    author="Kevin Kwok",
    author_email="antimatter15@gmail.com",
    description="Run Jupyter cells in AWS Lambda for massively parallel experimentation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/antimatter15/eigensheep",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Framework :: Jupyter",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers"
    ],
    install_requires=[
        'tqdm',
        'boto3',
        'ipython',
        'ipywidgets'
    ],
    extras_require={
        ':python_version == "2.7"': ['futures']
    }
)