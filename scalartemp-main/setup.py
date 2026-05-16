from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="persistent_context_engine",
    version="1.0.0",
    author="Hackathon Team",
    description="Persistent Context Engine for Autonomous SRE",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=["engine", "engine.*"]),
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "context-engine=engine.cli:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
