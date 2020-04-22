import logging

from dataiku.connector import Connector
from warp10 import Warp10Client, FetchModeParameters

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='Warp10 connector %(levelname)s - %(message)s')


class Warp10Connector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        warp10_connection = self.config.get('warp10_connection', None)
        if not warp10_connection:
            raise ValueError('No Warp10 connection defined')

        self.warp10_client = Warp10Client(warp10_connection)

        self.fetch_mode = self.config.get('fetch_mode', None)
        if not self.fetch_mode:
            raise ValueError('No fetch mode selected')

        self.start = self.config.get('start', None)
        self.stop = self.config.get('stop', None)
        self.now = self.config.get('now', None)
        self.timespan = self.config.get('timespan', None)

        self.fetch_mode_parameters = FetchModeParameters()
        if self.fetch_mode == 'interval':
            if not (self.start and self.stop):
                raise ValueError('Start or stop timestamp not defined for interval fetch mode')
            self.fetch_mode_parameters.as_interval(self.start, self.stop)
        elif self.fetch_mode == 'timespan':
            self.fetch_mode_parameters.as_timespan(self.now, self.timespan)
        else:
            raise ValueError('Unknown fetch mode: ' + self.fetch_mode)

        self.selector = self.config.get('selector', None)


    def get_read_schema(self):
        return {
            'columns':
            [
                {'name': 'timestamp', 'type': 'bigint'},
                {'name': 'latitude', 'type': 'double'},
                {'name': 'longitude', 'type': 'double'},
                {'name': 'elevation', 'type': 'bigint'},
                {'name': 'identifier', 'type': 'string'},
                {'name': 'value', 'type': 'string'},
                {'name': 'attributes', 'type': 'string'}
            ]
        }


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, 
                    partition_id=None, records_limit=-1):
        return self.warp10_client.generate_rows(self.selector, self.fetch_mode_parameters, records_limit)


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                partition_id=None):
        if dataset_schema is None or 'columns' not in dataset_schema:
            raise TypeError('Input dataset schema is missing')
        columns = dataset_schema['columns']
        if not any(column['name'] == 'timestamp' and column['type'] == 'bigint' for column in columns):
            raise TypeError('Input dataset schema is missing a "timestamp" column of type "bigint"')
        if not any(column['name'] == 'identifier' for column in columns):
            raise TypeError('Input dataset schema is missing an "identifier" column')
        if not any(column['name'] == 'value' for column in columns):
            raise TypeError('Input dataset schema is missing a "value" column')
        return Warp10Writer(self.warp10_client, dataset_schema)


class Warp10Writer:
    BATCH_SIZE = 10000  # Could make this a param...

    def __init__(self, warp10_client, dataset_schema):
        self.warp10_client = warp10_client
        self.dataset_schema = dataset_schema
        self.buffer = []

    def write_row(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.BATCH_SIZE:
            self._flush_buffer()

    def _flush_buffer(self):
        logger.info('Flushing Warp10 writer')
        self.warp10_client.write_rows(self.dataset_schema, self.buffer)
        self.buffer = []

    def close(self):
        logger.info('Closing Warp10 writer')
        self._flush_buffer()
