from setuptools import setup, find_packages
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()
setup(
    name="flowguard",
    version="1.0.0",
    description="Work Failure Detector — missed tasks, delays, ownership gaps",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OrbitScript/flowguard",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[],
    extras_require={"dev": ["pytest>=7.0"]},
    entry_points={"console_scripts": ["flowguard=flowguard.cli:main"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)
