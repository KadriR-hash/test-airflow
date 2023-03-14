from pydantic import BaseModel, Field, BaseConfig

BaseConfig.arbitrary_types_allowed = True


class DBConfigurator(BaseModel):
    connection_id: str
    conn_type: str
    description: str
    host: str
    login: str
    db_schema: str = Field(alias="schema")
    port: int
    password: str
    extra: str
