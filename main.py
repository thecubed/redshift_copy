import psycopg2
import multiprocessing
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-6s [ %(processName)-12s | %(funcName)-10s | %(name)-24s ] %(message)s')


class Worker(multiprocessing.Process):
    def __init__(self, config, schema_name, table_name):
        super(Worker, self).__init__()
        self.logger = logging.getLogger('{}.{}'.format(schema_name, table_name))
        self.config = config
        self.config['schema_name'] = schema_name
        self.config['table_name'] = table_name

    def run(self):
        """Run the migration - unload from source DB, then load (copy) to destination using S3"""
        try:
            self.logger.info("Starting unload...")
            self.unload()
            self.logger.info("Starting load...")
            self.load()
            self.logger.info("Thread finished!")
        except Exception as e:
            self.logger.error("Unable to proceed due to error. Thread ending.")

    def unload(self):
        """Connect to source database, copy all data from this instance of the worker's table parameter to S3"""
        # Source database
        connection = "host={} port=5439 dbname={} user={} password={}".format(self.config['source_host'],
                                                                              self.config['dbname'],
                                                                              self.config['source_user'],
                                                                              self.config['source_password'])
        self.logger.info("Connecting to: {}".format(connection))

        conn = psycopg2.connect(connection)
        cur = conn.cursor()

        # Get the columns we want from the source DB
        # cur.execute("select column_name from information_schema.columns where table_schema = %s and table_name = %s and (column_name != 'id' or column_name != 'id_table') order by ordinal_position;",
        #             (self.config['schema_name'], self.config['table_name']))
        #
        # self.config['columns'] = ",".join([x[0] for x in cur.fetchall()])

        # don't use psycopg2's formatter, since it'll quote everything - which is safe, but not what I want.
        #  I live on the edge!
        qry = "unload ('select * from {schema_name}.{table_name}') to 's3://{s3_bucket}/{schema_name}/{table_name}_' credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' escape;"\
            .format(**self.config)

        self.logger.debug("Executing: {}".format(qry))
        try:
            cur.execute(qry)
            self.logger.info("Command queued!")
        except psycopg2.Error as e:
            self.logger.error("Error in unload: \n{}".format(e))
            raise Exception("[UNLOAD] Unable to unload table")

        # Housekeeping
        cur.close()
        conn.close()

    def load(self):
        """Connect to destination database, copy from S3 into this instance's table parameter"""
        # Destination database
        connection = "host={} port=5439 dbname={} user={} password={}".format(self.config['dest_host'],
                                                                              self.config['dbname'],
                                                                              self.config['dest_user'],
                                                                              self.config['dest_password'])
        self.logger.info("Connecting to: {}".format(connection))
        conn = psycopg2.connect(connection)
        cur = conn.cursor()

        # Clear out the data from this table
        # u brave bro?
        # qry = "truncate {schema_name}.{table_name}".format(**self.config)
        # self.logger.info("Clearing destination table with query: {}".format(qry))
        # cur.execute(qry)
        # cur.fetchall()

        # Load new data from S3
        qry = "copy {schema_name}.{table_name} from 's3://{s3_bucket}/{schema_name}/{table_name}_' credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' escape explicit_ids;".format(**self.config)
        self.logger.debug("Executing: {}".format(qry))
        try:
            cur.execute(qry)
            conn.commit()
            for notice in conn.notices:
                self.logger.debug("Server notice: {}".format(notice))
            self.logger.info("Command queued!")
        except psycopg2.Error as e:
            self.logger.error("Error in load: \n{}".format(e))
            raise Exception("[LOAD] Unable to load table")

        # Housekeeping
        cur.close()
        conn.close()


def list_tables(config, schema):
    """List all tables on the source DB server in a given schema name"""
    logger = logging.getLogger()
    logger.info("Listing all tables in schema: %s", schema)

    connection = "host={} port=5439 dbname={} user={} password={}".format(config['source_host'],
                                                                          config['dbname'],
                                                                          config['source_user'],
                                                                          config['source_password'])
    logger.info("Connecting to: {}".format(connection))
    conn = psycopg2.connect(connection)
    cur = conn.cursor()

    cur.execute("select table_name from information_schema.tables where table_schema = %s and table_type = 'BASE TABLE';", (schema,))
    return [x[0] for x in cur.fetchall()]


def do_migrate(config, schema_name):
    """Main function to call to migrate tables in a schema"""
    tables = list_tables(baseconfig, schema_name)

    threads = []

    for table in tables:
        logging.info("Starting worker for table: {}".format(table))
        t = Worker(config, schema_name, table)
        t.start()
        threads.append(t)

    logging.info("Waiting for threads to finish...")
    for thread in threads:
        thread.join()

    logging.info("All threads finished. KThxBye!")


if __name__ == "__main__":
    import ConfigParser

    cp = ConfigParser.ConfigParser()
    cp.read('config.ini')

    baseconfig = None
    try:
        baseconfig = {
            # AWS
            's3_bucket': cp.get('aws', 's3_bucket'),
            'access_key': cp.get('aws', 'access_key'),
            'secret_key': cp.get('aws', 'secret_key'),
            # Redshift
            'source_host': cp.get('redshift', 'source_host'),
            'source_user': cp.get('redshift', 'source_user'),
            'source_password': cp.get('redshift', 'source_password'),
            'dest_host': cp.get('redshift', 'dest_host'),
            'dest_user': cp.get('redshift', 'dest_user'),
            'dest_password': cp.get('redshift', 'dest_password'),
            'dbname': cp.get('redshift', 'dbname'),
            'schema': cp.get('redshift', 'schema')
        }
    except Exception as e:
        print("Error reading config file! Cannot continue.")
        print(e)

    do_migrate(baseconfig, baseconfig['schema'])
