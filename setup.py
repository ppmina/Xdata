from setuptools import setup

setup(
    # ... other settings ...
    package_data={"cryptoservice": ["py.typed"]},
    zip_safe=False,  # Required for mypy to find py.typed file
)
