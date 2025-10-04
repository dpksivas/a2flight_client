from setuptools import setup

setup(
    name="a2flight-client",
    version="2.3.0",
    py_modules=[
        'a2flight_client',
        'a2flight_cl_config',
        'a2flight_cl_env',
    ],
    install_requires=[
        "pyarrow[flight]>=14.0.0",
        "rich>=13.0.0",
    ],
    python_requires=">=3.8",
)