from typing import Dict, List, Optional, Union, Tuple

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, DuplicateKeyError
from sshtunnel import SSHTunnelForwarder

from blocks.infrastructure import InfrastructureConfig
from common.logging import get_logger
from common.config import config

def handle_bulk_write_error(bwe: BulkWriteError, collection_name: str, logger) -> int:
    write_errors = bwe.details.get("writeErrors", [])
    duplicate_key_errors = [
        error for error in write_errors if error.get("code") == 11000
    ]
    other_errors = [error for error in write_errors if error.get("code") != 11000]

    if duplicate_key_errors:
        logger.warning(
            f"Ignored {len(duplicate_key_errors)} duplicate key errors in {collection_name}."
        )
    if other_errors:
        logger.error(
            f"Bulk write operation failed with other errors in {collection_name}: {len(other_errors)} errors"
        )
        # Log a sample of failed documents
        if other_errors:
            sample_error = other_errors[0]
            logger.error(f"Sample error: {sample_error.get('errmsg')}")
            logger.error(f"Failed document: {sample_error.get('op')}")

    return bwe.details.get("nInserted", 0)
class MongoDBClient:
    """
    Synchronous MongoDB client with SSH tunnel support.

    This client provides a comprehensive interface for working with MongoDB,
    with support for SSH tunneling, connection pooling, and all common
    MongoDB operations (find, insert, update, delete, aggregate).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        ssh_host: Optional[str] = None,
        ssh_port: Optional[int] = None,
        ssh_username: Optional[str] = None,
        ssh_password: Optional[str] = None,
        default_collection: Optional[str] = None,
        use_config: bool = False,
        use_block: str = None,
    ):
        """
        Initialize MongoDB client with optional SSH tunnel.

        Args:
            host: MongoDB server hostname
            port: MongoDB server port
            username: MongoDB username
            password: MongoDB password
            database: Database name to connect to
            ssh_host: SSH tunnel hostname (if needed)
            ssh_port: SSH tunnel port (default: 22)
            ssh_username: SSH username (if needed)
            ssh_password: SSH password (if needed)
            default_collection: Default collection name (optional)
            use_config: Whether to try loading values from config if params are None
            use_block: Whether to use Prefect block to retrieve secret information
        """
        self.logger = get_logger()

        if use_config:
            # MongoDB connection info
            self.host = host or config("MONGODB_HOST")
            self.port = int(port or config("MONGODB_PORT", 27017))
            self.username = username or config("MONGODB_USERNAME")
            self.password = password or config("MONGODB_PWD")
            self.database = database or config("MONGODB_DBNAME")
            self.default_collection_name = default_collection or config(
                "MONGODB_COLLECTION"
            )

            # SSH tunnel info
            self.ssh_host = ssh_host or config("SSH_HOST")
            self.ssh_port = int(ssh_port or config("SSH_PORT", 22))
            self.ssh_username = ssh_username or config("SSH_USERNAME")
            self.ssh_password = ssh_password or config("SSH_PWD")
        elif use_block:
            # Using Prefect block to retrieve connection details
            config_block = InfrastructureConfig.load(use_block)
            config_details = config_block.get_connection_details()

            if config_details["type"] != "mongodb":
                raise ValueError(f"Config '{use_block}' is not a MongoDB configuration")

            # Set MongoDB connection parameters with fallbacks
            self.host = host or config_details.get("host") or config("MONGODB_HOST")
            self.port = int(
                port or config_details.get("port") or config("MONGODB_PORT", 27017)
            )
            self.username = (
                username or config_details.get("username") or config("MONGODB_USERNAME")
            )
            self.password = (
                password or config_details.get("password") or config("MONGODB_PWD")
            )
            self.database = (
                database or config_details.get("database") or config("MONGODB_DATABASE")
            )
            self.default_collection_name = (
                default_collection
                or config_details.get("default_collection")
                or config("MONGODB_COLLECTION")
            )

            # Get SSH tunnel details from a dedicated SSH block
            try:
                ssh_config_block = InfrastructureConfig.load("ssh-mongodb")
                ssh_config_details = ssh_config_block.get_connection_details()

                self.ssh_host = (
                    ssh_host or ssh_config_details.get("host") or config("SSH_HOST")
                )
                self.ssh_port = int(
                    ssh_port or ssh_config_details.get("port") or config("SSH_PORT", 22)
                )
                self.ssh_username = (
                    ssh_username
                    or ssh_config_details.get("username")
                    or config("SSH_USERNAME")
                )
                self.ssh_password = (
                    ssh_password
                    or ssh_config_details.get("password")
                    or config("SSH_PWD")
                )
            except Exception as e:
                self.logger.warning(
                    f"Couldn't load SSH details from block: {e}. Will try direct connection."
                )
                self.ssh_host = ssh_host or config("SSH_HOST")
                self.ssh_port = int(ssh_port or config("SSH_PORT", 22))
                self.ssh_username = ssh_username or config("SSH_USERNAME")
                self.ssh_password = ssh_password or config("SSH_PWD")
        else:
            # Use provided values directly
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.database = database
            self.default_collection_name = default_collection

            self.ssh_host = ssh_host
            self.ssh_port = ssh_port
            self.ssh_username = ssh_username
            self.ssh_password = ssh_password

        # Initialize connection objects
        self.ssh_tunnel = None
        self.client = None
        self.db = None
        self.collection = None

    @classmethod
    def from_block(cls, block_name, **kwargs):
        """Create a MongoDBClient from a Prefect block configuration."""
        client = cls(use_block=block_name, **kwargs)
        client.connect()
        return client

    def __enter__(self):
        """Support for context manager."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connections when exiting context."""
        self.close()

    def connect(self) -> None:
        """
        Establish connection to MongoDB, optionally through SSH tunnel.

        Raises:
            Exception: If connection fails
        """
        if self.client is not None:
            return  # Already connected

        # Validate connection parameters
        if not all([self.host, self.database, self.username, self.password]):
            missing = []
            if not self.host:
                missing.append("host")
            if not self.database:
                missing.append("database")
            if not self.username:
                missing.append("username")
            if not self.password:
                missing.append("password")
            raise ValueError(
                f"Missing required connection parameters: {', '.join(missing)}"
            )

        # Create SSH tunnel if needed
        connection_host = self.host
        connection_port = self.port

        if self.ssh_host and self.ssh_username and self.ssh_password:
            self.logger.info(f"Creating SSH tunnel to {self.ssh_host}:{self.ssh_port}")
            try:
                if not self.ssh_tunnel:
                    self.ssh_tunnel = self._create_ssh_tunnel(
                        self.ssh_host,
                        self.ssh_username,
                        self.ssh_password,
                        self.ssh_port,
                        self.host,
                        self.port,
                    )

                connection_host = "localhost"
                connection_port = self.ssh_tunnel.local_bind_port
                self.logger.info(f"Using local SSH tunnel port: {connection_port}")
            except Exception as e:
                self.logger.error(f"Failed to create SSH tunnel: {e}")
                raise
        else:
            self.logger.info(
                f"Connecting directly to MongoDB at {connection_host}:{connection_port}"
            )

        # Create MongoDB connection string
        mongo_uri = (
            f"mongodb://{self.username}:{self.password}@"
            f"{connection_host}:{connection_port}/{self.database}"
            f"?authSource=admin&socketTimeoutMS=86400000&connectTimeoutMS=86400000"
        )

        # Connect to MongoDB
        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client[self.database]

            # Set default collection if provided
            if self.default_collection_name:
                self.collection = self.db[self.default_collection_name]

            # Test connection by requesting server info
            self.client.admin.command("ping")
            self.logger.info(f"Connected to MongoDB: {self.database}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            if self.ssh_tunnel:
                self.ssh_tunnel.close()
                self.ssh_tunnel = None
            self.client = None
            self.db = None
            self.collection = None
            raise

    def set_collection(self, collection_name: str) -> Collection:
        """
        Set the active collection.

        Args:
            collection_name: Name of the collection to use

        Returns:
            The collection object

        Raises:
            RuntimeError: If not connected yet
        """
        if self.db is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")

        self.collection = self.db[collection_name]
        return self.collection

    def ensure_collection_exists(self, collection_name: str) -> bool:
        """
        Ensure a collection exists by checking if it's in the list of collections.

        MongoDB will create collections automatically on first insert, but this
        method provides explicit validation.

        Args:
            collection_name: Name of the collection to check

        Returns:
            True if collection exists or will be created, False on error
        """
        if self.db is None:
            self.connect()

        try:
            collections = self.db.list_collection_names()
            if collection_name not in collections:
                self.logger.info(
                    f"Collection '{collection_name}' doesn't exist yet, it will be created on first insert"
                )
            return True
        except Exception as e:
            self.logger.error(
                f"Error checking if collection {collection_name} exists: {e}"
            )
            return False

    def _get_collection(self, collection_name: Optional[str] = None) -> Collection:
        """
        Get a collection object, using either specified name or default collection.

        Args:
            collection_name: Optional collection name

        Returns:
            MongoDB collection

        Raises:
            ValueError: If no collection specified and no default collection
        """
        if collection_name:
            return self.db[collection_name]
        elif self.collection is not None:
            return self.collection
        else:
            raise ValueError("No collection specified and no default collection set")

    def insert_one(
        self,
        document: Dict,
        collection_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Insert a single document into MongoDB.

        Args:
            document: Document to insert
            collection_name: Optional collection name

        Returns:
            Inserted document ID or None on error

        Raises:
            Exception: On connection or insertion error
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        try:
            result = collection.insert_one(document)
            self.logger.info(
                f"Document inserted into {collection.name} with ID: {result.inserted_id}"
            )
            return str(result.inserted_id)
        except DuplicateKeyError:
            self.logger.warning(
                f"Document with duplicate key not inserted into {collection.name}"
            )
            return None
        except Exception as e:
            self.logger.error(f"Error inserting document into {collection.name}: {e}")
            raise

    def insert_many(
        self,
        data_batch: List[Dict],
        collection_name: Optional[str] = None,
        ordered: bool = False,
    ) -> int:
        """
        Insert multiple documents into a MongoDB collection.

        Args:
            data_batch: List of documents to insert
            collection_name: Optional collection name (uses default if not specified)
            ordered: Whether to perform an ordered insert

        Returns:
            Number of documents inserted

        Raises:
            Exception: If insertion fails
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        if not data_batch:
            self.logger.warning("Attempted to insert empty batch, skipping")
            return 0

        try:
            self.logger.info(
                f"Inserting batch of size {len(data_batch)} into MongoDB collection '{collection.name}'"
            )
            result = collection.insert_many(data_batch, ordered=ordered)
            inserted_count = len(result.inserted_ids)
            self.logger.info(f"Batch inserted successfully. Inserted: {inserted_count}")
            return inserted_count

        except BulkWriteError as bwe:
            inserted_count = handle_bulk_write_error(bwe, collection.name, self.logger)
            return inserted_count

        except Exception as e:
            self.logger.error(
                f"An error occurred during bulk insert to {collection.name}: {e}"
            )
            raise

    def find(
        self,
        query: Dict = None,
        projection: Dict = None,
        collection_name: Optional[str] = None,
        limit: int = 0,
        skip: int = 0,
        sort: List = None,
        no_cursor_timeout: bool = False,
    ) -> List[Dict]:
        """
        Find documents in MongoDB.

        Args:
            query: MongoDB query filter
            projection: Fields to include/exclude
            collection_name: Optional collection name
            limit: Maximum number of results (0 = no limit)
            skip: Number of documents to skip
            sort: Sort specification
            no_cursor_timeout: Prevent cursor from timing out

        Returns:
            List of matching documents
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        # Set default query if none provided
        query = query or {}

        # Use session if no_cursor_timeout is True
        if no_cursor_timeout:
            with self.client.start_session() as session:
                cursor = collection.find(query, projection, session=session)

                if skip > 0:
                    cursor = cursor.skip(skip)

                if limit > 0:
                    cursor = cursor.limit(limit)

                if sort:
                    cursor = cursor.sort(sort)

                # Convert cursor to list within session
                results = list(cursor)
        else:
            cursor = collection.find(query, projection)

            if skip > 0:
                cursor = cursor.skip(skip)

            if limit > 0:
                cursor = cursor.limit(limit)

            if sort:
                cursor = cursor.sort(sort)

            # Convert cursor to list
            results = list(cursor)

        self.logger.info(f"Found {len(results)} documents in {collection.name}")
        return results

    def find_one(
        self,
        query: Dict,
        projection: Dict = None,
        collection_name: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Find a single document in MongoDB.

        Args:
            query: MongoDB query filter
            projection: Fields to include/exclude
            collection_name: Optional collection name

        Returns:
            Matching document or None if not found
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        result = collection.find_one(query, projection)
        if result:
            self.logger.info(f"Found document in {collection.name}")
        else:
            self.logger.info(f"No document found in {collection.name} matching query")
        return result

    def update_one(
        self,
        query: Dict,
        update: Dict,
        collection_name: Optional[str] = None,
        upsert: bool = False,
    ) -> Dict:
        """
        Update a single document in MongoDB.

        Args:
            query: MongoDB query filter
            update: Update operations
            collection_name: Optional collection name
            upsert: Whether to insert if not exists

        Returns:
            Update result details
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        result = collection.update_one(query, update, upsert=upsert)

        update_info = {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": result.upserted_id,
        }

        self.logger.info(
            f"Updated document in {collection.name}: "
            f"matched={update_info['matched_count']}, "
            f"modified={update_info['modified_count']}, "
            f"upserted={'Yes' if update_info['upserted_id'] else 'No'}"
        )

        return update_info

    def update_many(
        self,
        query: Dict,
        update: Dict,
        collection_name: Optional[str] = None,
        upsert: bool = False,
    ) -> Dict:
        """
        Update multiple documents in MongoDB.

        Args:
            query: MongoDB query filter
            update: Update operations
            collection_name: Optional collection name
            upsert: Whether to insert if not exists

        Returns:
            Update result details
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        result = collection.update_many(query, update, upsert=upsert)

        update_info = {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_count": 1 if result.upserted_id else 0,
        }

        self.logger.info(
            f"Updated documents in {collection.name}: "
            f"matched={update_info['matched_count']}, "
            f"modified={update_info['modified_count']}, "
            f"upserted={'Yes' if result.upserted_id else 'No'}"
        )

        return update_info

    def delete_one(self, query: Dict, collection_name: Optional[str] = None) -> int:
        """
        Delete a single document from MongoDB.

        Args:
            query: MongoDB query filter
            collection_name: Optional collection name

        Returns:
            Number of documents deleted
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        result = collection.delete_one(query)
        deleted_count = result.deleted_count

        self.logger.info(f"Deleted {deleted_count} document from {collection.name}")
        return deleted_count

    def delete_many(self, query: Dict, collection_name: Optional[str] = None) -> int:
        """
        Delete multiple documents from MongoDB.

        Args:
            query: MongoDB query filter
            collection_name: Optional collection name

        Returns:
            Number of documents deleted
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        result = collection.delete_many(query)
        deleted_count = result.deleted_count

        self.logger.info(f"Deleted {deleted_count} documents from {collection.name}")
        return deleted_count

    def aggregate(
        self,
        pipeline: List[Dict],
        collection_name: Optional[str] = None,
        allow_disk_use: bool = True,
        no_cursor_timeout: bool = False,
    ) -> List[Dict]:
        """
        Execute an aggregation pipeline.

        Args:
            pipeline: MongoDB aggregation pipeline
            collection_name: Optional collection name
            allow_disk_use: Whether to allow disk use for large operations
            no_cursor_timeout: Prevent cursor from timing out

        Returns:
            Aggregation results
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        self.logger.info(f"Executing aggregation pipeline on {collection.name}")

        if no_cursor_timeout:
            with self.client.start_session() as session:
                cursor = collection.aggregate(
                    pipeline, allowDiskUse=allow_disk_use, session=session
                )
                results = list(cursor)
        else:
            cursor = collection.aggregate(pipeline, allowDiskUse=allow_disk_use)
            results = list(cursor)

        self.logger.info(f"Aggregation returned {len(results)} results")
        return results

    def count_documents(
        self, query: Dict = None, collection_name: Optional[str] = None
    ) -> int:
        """
        Count documents matching a query.

        Args:
            query: MongoDB query filter
            collection_name: Optional collection name

        Returns:
            Document count
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)
        query = query or {}

        count = collection.count_documents(query)
        self.logger.info(f"Counted {count} documents in {collection.name}")
        return count

    def create_index(
        self,
        keys: Union[str, List[Tuple[str, int]]],
        collection_name: Optional[str] = None,
        unique: bool = False,
        background: bool = True,
    ) -> str:
        """
        Create an index on a collection.

        Args:
            keys: Index keys (either a single field name or list of (field, direction) tuples)
            collection_name: Optional collection name
            unique: Whether the index should enforce uniqueness
            background: Whether to build the index in the background

        Returns:
            Name of the created index
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        # Convert string to single-key index spec
        if isinstance(keys, str):
            keys = [(keys, 1)]

        options = {"unique": unique, "background": background}

        index_name = collection.create_index(keys, **options)
        self.logger.info(f"Created index '{index_name}' on {collection.name}")
        return index_name

    def _create_ssh_tunnel(
        self,
        ssh_host: str,
        ssh_username: str,
        ssh_password: str,
        ssh_port: int,
        mongo_host: str,
        mongo_port: int,
    ) -> SSHTunnelForwarder:
        """
        Create an SSH tunnel for MongoDB connection.

        Args:
            ssh_host: SSH server hostname
            ssh_username: SSH username
            ssh_password: SSH password
            ssh_port: SSH port
            mongo_host: MongoDB hostname as seen from SSH server
            mongo_port: MongoDB port

        Returns:
            SSHTunnelForwarder instance
        """
        ssh_tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=(mongo_host, mongo_port),
            set_keepalive=60.0,
            allow_agent=False,
            ssh_pkeys=[],
        )
        ssh_tunnel.start()
        self.logger.info(
            f"SSH tunnel established to {ssh_host}:{ssh_port} -> {mongo_host}:{mongo_port}"
        )
        return ssh_tunnel

    def upsert_many(
            self,
            data_batch: List[Dict],
            unique_key: str = "_id",  # Change default to _id
            collection_name: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Upsert multiple documents into MongoDB using _id as the unique identifier.

        Args:
            data_batch: List of documents to upsert
            unique_key: Field to use as unique identifier (default: "_id")
            collection_name: Optional collection name

        Returns:
            Dictionary with counts of upserted and updated documents
        """
        if self.db is None:
            self.connect()

        collection = self._get_collection(collection_name)

        if not data_batch:
            self.logger.warning("Attempted to upsert empty batch, skipping")
            return {"inserted": 0, "updated": 0, "total": 0}

        try:
            self.logger.info(
                f"Upserting batch of size {len(data_batch)} into MongoDB collection '{collection.name}'"
            )

            # Create a list of ReplaceOne operations using _id
            operations = []
            for doc in data_batch:
                if unique_key not in doc:
                    self.logger.warning(
                        f"Document missing unique key '{unique_key}', skipping"
                    )
                    continue

                # Use _id for upsert operation
                operations.append(
                    pymongo.ReplaceOne(
                        {unique_key: doc[unique_key]},  # Filter by _id
                        doc,  # Replacement document
                        upsert=True,  # Insert if not exists, update if exists
                    )
                )

            if not operations:
                self.logger.warning("No valid operations created, skipping batch")
                return {"inserted": 0, "updated": 0, "total": 0}

            # Execute the bulk operation
            result = collection.bulk_write(operations, ordered=False)

            # Get operation counts
            stats = {
                "inserted": result.upserted_count,
                "updated": result.modified_count,
                "total": result.upserted_count + result.modified_count,
            }

            self.logger.info(
                f"Upsert completed: {stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['total']} total records in {collection.name}"
            )

            return stats

        except Exception as e:
            self.logger.error(
                f"Error during upsert operation in {collection.name}: {e}"
            )
            raise

    def close(self) -> None:
        """Close MongoDB connection and SSH tunnel."""
        if hasattr(self, "client") and self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.collection = None
            self.logger.info("MongoDB client closed")

        if hasattr(self, "ssh_tunnel") and self.ssh_tunnel:
            self.ssh_tunnel.close()
            self.ssh_tunnel = None
            self.logger.info("SSH tunnel closed")
