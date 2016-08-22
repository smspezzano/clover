# -*- coding: utf-8 -*-

import psycopg2
from settings import RDS_NAME, RDS_USERNAME, RDS_HOSTNAME, RDS_PASSWORD


def get_connection():
    try:
        return psycopg2.connect("dbname={0} user={1} host={2} password={3}".format(
            RDS_NAME, RDS_USERNAME, RDS_HOSTNAME, RDS_PASSWORD
        ))
    except psycopg2.Error as e:
        print e
        return False
