import psycopg
from psycopg.sql import Identifier, SQL

columns = ['a1', 'b2']
psy_cols = SQL(', ').join([Identifier(column) for column in columns])
psy_values = SQL(', ').join([SQL("%s")] * len(columns))

insert_query = SQL("INSERT INTO {table} ({columns_s}) VALUES ({values});").format(
    table=Identifier("sdsd.tefv"),
    columns_s=psy_cols,
    values=psy_values
)
print(insert_query.as_string())
print(insert_query)