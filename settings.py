import os

empty = object()


def environ(key, default=empty):
    try:
        return os.environ[key]
    except KeyError:
        if default is empty:
            raise RuntimeError('environment variable "{0}s" does not exist'.format(key))
        return default

TARGET = environ("TARGET")
DIRECTORY_BASE_PATH = environ("DIRECTORY_BASE_PATH")

if TARGET == "development":
    # Database
    RDS_NAME = environ("RDS_DB_NAME")
    RDS_USERNAME = environ("RDS_USERNAME")
    RDS_PASSWORD = environ("RDS_PASSWORD")
    RDS_HOSTNAME = environ("RDS_HOSTNAME")

    # File paths
    DATA_PATH = environ("DATA_PATH")
    SPECS_PATH = environ("SPECS_PATH")
    PARSED_DATA_PATH = environ("PARSED_DATA_PATH")
elif TARGET == "testing":
    RDS_NAME = environ("TEST_RDS_DB_NAME")
    RDS_USERNAME = environ("TEST_RDS_USERNAME")
    RDS_PASSWORD = environ("TEST_RDS_PASSWORD")
    RDS_HOSTNAME = environ("TEST_RDS_HOSTNAME")

    # Test file paths
    DATA_PATH = environ("TEST_DATA_PATH")
    SPECS_PATH = environ("TEST_SPECS_PATH")
    PARSED_DATA_PATH = environ("TEST_PARSED_DATA_PATH")
else:
    raise RuntimeError(
        'Target: "{0}" does not exist. Check $TARGET'.format(TARGET)
    )
