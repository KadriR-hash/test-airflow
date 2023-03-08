from enum import Enum


class Operators(str, Enum):
    POSTGRES_REQUEST = "POSTGRES_REQUEST"
    GET_DATA_FROM_URL_TO_FILE = "GET_DATA_FROM_URL_TO_FILE"
    UPDATE_DATA_FROM_URL_TO_FILE = "UPDATE_DATA_FROM_URL_TO_FILE"
    INSERT_DATA_FROM_FILE_INTO_DB = "INSERT_DATA_FROM_FILE_INTO_DB"
    CLEAN_DB = "CLEAN_DB"
    CLEAN_FILE = "CLEAN_FILE"
    BRANCH_UNDER_CONDITION = "BRANCH_UNDER_CONDITION"

    """POSTGRES_REQUEST = {'postgres_conn_id': '', 'sql': '', 'trigger_rule': ''}
    GET_DATA_FROM_URL_TO_FILE = {}
    UPDATE_DATA_FROM_URL_TO_FILE = {'postgres_conn_id': ''}
    INSERT_DATA_FROM_FILE_INTO_DB = {'postgres_conn_id': ''}
    CLEAN_DB = {'postgres_conn_id': '', 'sql': '', 'trigger_rule': ''}
    CLEAN_FILE = {'postgres_conn_id': '', 'trigger_rule': ''}
    BRANCH_UNDER_CONDITION = {}"""
