#!/usr/bin/env python3
"""
    Purpose:
        Connector Library for MySQL. Will provide a decorator for initiating a db_cur
        and injecting it as a parameter into a fuction.
"""

# Python Library Imports
import logging
import wrapt
import pymysql


def mysql_connector(hostname, username, password, port=3306):
    """
        Purpose:
            Decorator for connecting to mysql database
        Args:
            hostname (string): hostname of server
            username (string): username to connect to db with
            password (string): password of user
            port (int): port number for mysql instance
        Returns:
            decorator (function): function decorating another
                function and injecting a db_cur for conencting,
                committing, and closing the db connection
    """

    @wrapt.decorator
    def with_connection(f, instance, args, kwargs):
        """
            Purpose:
                Database connection wrapping function
            Args:
                f (function/method): function being decorated
                instance: pass in self when wraping class method.
                    default is None when wraping function.
                args (Tuple): List of arguments
                kwargs (Dict): Dictionary of named arguments
            Return:
                db_cur (Mysql Database Cursor): Cursor connected to
                    sopecified database
            Function Termination:
                Will close connection to the database
        """

        logging.info(
            'Connecting to MySQL: {host}:{port}'.format(
                host=hostname, port=port
            )
        )

        db_con = pymysql.connect(
            host=hostname,
            port=port,
            user=username,
            passwd=password,
            local_infile=True)
        db_con.autocommit(1)

        db_cur = db_con.cursor(pymysql.cursors.DictCursor)

        try:
            output = f(db_cur, *args, **kwargs)

        except Exception as err:
            logging.error('Error Connecting to MySQL: {0}'.format(err))
            raise err

        finally:
            logging.info('Closing Database Cursor and Connection')
            db_cur.close()
            db_con.close()

        return output

    return with_connection
