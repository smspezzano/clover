**Clover -- Parse File**

This app takes data and spec files and creates / updated PostgreSQL tables with the data.

Files are put into /data and /specs

**Set up**:
 - Recommended:
    - [Virtualenvwrapper] (https://virtualenvwrapper.readthedocs.io/en/latest/)
    - [Autoenv] (https://github.com/kennethreitz/autoenv)
- Required:
      - python > 2.7.X
      - PostgreSQL > 9.X.X
      - psycopg2 == 2.6.2
    
- Create DB and update variables in .env

**Tests**:
 - To run `python file_parser_tests.py`
 