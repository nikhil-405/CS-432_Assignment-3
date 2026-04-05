from .table import Table

class DatabaseManager:
    # Initialize the in-memory registry of databases and their tables.
    def __init__(self):
        self.databases = {}

    # Create a new database entry if its name is not already used.
    def create_database(self, db_name):
        if db_name in self.databases:
            return False
        self.databases[db_name] = {}
        return True

    # Delete an existing database by name.
    def delete_database(self, db_name):
        if db_name not in self.databases:
            return False
        del self.databases[db_name]
        return True

    # Return all database names in sorted order.
    def list_databases(self):
        return sorted(self.databases.keys())

    # Create a table in a database using the provided schema and indexing options.
    def create_table(self, db_name, table_name, schema, order=8, search_key=None):
        if db_name not in self.databases:
            raise KeyError(f"database '{db_name}' does not exist")

        if table_name in self.databases[db_name]:
            return False

        self.databases[db_name][table_name] = Table(
            name=table_name,
            schema=schema,
            order=order,
            search_key=search_key,
        )
        return True

    # Remove a table from the specified database.
    def delete_table(self, db_name, table_name):
        if db_name not in self.databases:
            raise KeyError(f"database '{db_name}' does not exist")

        if table_name not in self.databases[db_name]:
            return False

        del self.databases[db_name][table_name]
        return True

    # Return all table names for a database in sorted order.
    def list_tables(self, db_name):
        if db_name not in self.databases:
            raise KeyError(f"database '{db_name}' does not exist")
        return sorted(self.databases[db_name].keys())

    # Retrieve a table object by database and table name.
    def get_table(self, db_name, table_name):
        if db_name not in self.databases:
            raise KeyError(f"database '{db_name}' does not exist")
        return self.databases[db_name].get(table_name)

