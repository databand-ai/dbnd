# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow import settings
from airflow.models import Connection
from six.moves.urllib.parse import urlunparse
from sqlalchemy.orm import exc

from dbnd._core.errors import DatabandRuntimeError


logger = logging.getLogger(__name__)


def delete_connection(conn_id):
    session = settings.Session()
    try:
        to_delete = (
            session.query(Connection).filter(Connection.conn_id == conn_id).one()
        )
    except exc.NoResultFound:
        logger.info("Did not find a connection with `conn_id`=%s\n'", conn_id)
    except exc.MultipleResultsFound:
        raise DatabandRuntimeError(
            "Found more than one connection with "
            + "`conn_id`={conn_id}\n".format(conn_id=conn_id)
        )
    else:
        session.delete(to_delete)
        session.commit()
        logger.info("Successfully deleted `conn_id`=%s\n", conn_id)


def add_connection(
    conn_id,
    uri=None,
    conn_type=None,
    host=None,
    login=None,
    password=None,
    port=None,
    schema=None,
    extra=None,
):
    if uri:
        new_conn = Connection(conn_id=conn_id, uri=uri)
    else:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port,
        )
    if extra is not None:
        new_conn.set_extra(extra)

    session = settings.Session()
    if not (
        session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()
    ):
        session.add(new_conn)
        session.commit()
        msg = "\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n"
        msg = msg.format(
            conn_id=new_conn.conn_id,
            uri=uri
            or urlunparse(
                (
                    conn_type,
                    "{login}:{password}@{host}:{port}".format(
                        login=login or "",
                        password=password or "",
                        host=host or "",
                        port=port or "",
                    ),
                    schema or "",
                    "",
                    "",
                    "",
                )
            ),
        )
        logger.info(msg)
        logger.info("extra=%s" % str(extra))
    else:
        msg = "\n\tA connection with `conn_id`={conn_id} already exists\n"
        raise DatabandRuntimeError(msg.format(conn_id=new_conn.conn_id))


def set_connection(
    conn_id,
    uri=None,
    conn_type=None,
    host=None,
    login=None,
    password=None,
    port=None,
    schema=None,
    extra=None,
):
    delete_connection(conn_id)
    add_connection(
        conn_id=conn_id,
        uri=uri,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        schema=schema,
        extra=extra,
    )
