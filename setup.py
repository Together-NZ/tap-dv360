from setuptools import setup, find_packages

setup(
    name="dv360-121",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "singer-sdk",
        "google-auth",
        "google-auth-oauthlib",
        "google-api-python-client",
    ],
    include_package_data=True,
)
