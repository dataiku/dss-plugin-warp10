import logging
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='Warp10 client %(levelname)s - %(message)s')


class WarpScriptExecutionError(Exception):
    pass


class FetchModeParameters:
    def __init__(self):
        self.fetch_mode = None
        self.start = None
        self.stop = None
        self.now = None
        self.timespan = None

    def as_interval(self, start, stop):
        self.fetch_mode = 'interval'
        self.start = start
        self.stop = stop

    def as_timespan(self, now, timespan):
        self.fetch_mode = 'timespan'
        self.now = now
        self.timespan = timespan

    def is_interval(self):
        return self.fetch_mode == 'interval'

    def is_timespan(self):
        return self.fetch_mode == 'timespan'


class GTSStringBuilder:
    GTS_INPUT_FORMAT = '{timestamp}/{latlon}/{elevation} {identifier} {value}'

    def __init__(self):
        self.timestamp = None
        self.latitude = None
        self.longitude = None
        self.elevation = ''
        self.identifier = None
        self.value = None
        self.value_type = None

    def with_timestamp(self, timestamp):
        self.timestamp = timestamp

    def with_latitude(self, latitude):
        self.latitude = latitude

    def with_longitude(self, longitude):
        self.longitude = longitude

    def with_elevation(self, elevation):
        self.elevation = elevation

    def with_identifier(self, identifier):
        self.identifier = identifier

    def with_value(self, value, value_type):
        self.value = value
        self.value_type = value_type

    def to_string(self):
        # Better to throw if something is missing, or just log and skip this row?
        if not self.identifier:
            raise TypeError('Row is missing an identifier')
        if not self.timestamp:
            raise TypeError('Row is missing a timestamp')
        if not self.value:
            raise TypeError('Row is missing a value')
        
        latlon = ''
        if self.latitude and self.longitude:
            latlon = str(self.latitude) + ':' + str(self.longitude)

        if not ('{' in self.identifier and '}' in self.identifier):  # Very dirty check for x{y}
            self.identifier = self.identifier + '{}'

        if self.value_type == 'string':
            self.value = '"' + self.value + '"'

        return self.GTS_INPUT_FORMAT.format(timestamp=self.timestamp, latlon=latlon, elevation=self.elevation, 
                                            identifier=self.identifier, value=self.value)


class Warp10Client:
    ALL_SELECTOR = '~.*{}'
    MAX_LONG = 9223372036854775807

    def __init__(self, warp10_connection):
        self.base_url = warp10_connection['base_url'] + '/api/v0/'
        self.verify_ssl_cert = not warp10_connection.get('ignore_ssl_certificate_verification')
        self.read_token = warp10_connection['read_token']
        self.write_token = warp10_connection['write_token']

    # Returns body as JSON
    def _fetch(self, selector, fetch_mode_parameters, records_limit):
        headers = {'X-Warp10-Token': self.read_token}
        params = {
            'selector': selector if selector else self.ALL_SELECTOR,
            'format': 'json'
        }

        if fetch_mode_parameters.is_interval():
            params['start'] = fetch_mode_parameters.start
            params['stop'] = fetch_mode_parameters.stop
        elif fetch_mode_parameters.is_timespan():
            params['now'] = fetch_mode_parameters.now if fetch_mode_parameters.now else self.MAX_LONG
            timespan = fetch_mode_parameters.timespan if fetch_mode_parameters.timespan else -self.MAX_LONG
            try:
                timespan = int(timespan)
                if records_limit > 0 and abs(timespan) > records_limit:
                    timespan = -records_limit
            except ValueError:
                pass

            params['timespan'] = timespan

        logger.info('Fetching data from Warp10 with parameters: {}'.format(params))
        response = requests.get(self.base_url + 'fetch', headers=headers, params=params, verify=self.verify_ssl_cert)
        if not response.ok:
            response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            # No JSON decoded, probably no rows fetched
            logger.warn('No data fetched from Warp10')
            return []

    def _update(self, payload):
        headers = {'X-Warp10-Token': self.write_token}
        logger.info('Posting data to Warp10')
        response = requests.post(self.base_url + 'update', headers=headers, data=payload, verify=self.verify_ssl_cert)
        if not response.ok:
            response.raise_for_status()

    def convert_dict_to_string(self, elem):
        return '{' + ','.join("%s=%s" % (key, value) for (key, value) in elem.items()) + '}'

    def generate_rows(self, selector, fetch_mode_parameters, records_limit):
        chunks_json = self._fetch(selector, fetch_mode_parameters, records_limit)

        chunk_metas = {}

        for chunk in chunks_json:
            chunk_id = chunk.get('i', None)
            if chunk_id not in chunk_metas:
                chunk_metas[chunk_id] = {
                    'name': chunk.get('c', None),
                    'labels': chunk.get('l', {}).copy(),
                    'attributes': chunk.get('a', {}).copy()
                }
            chunk_meta = chunk_metas[chunk_id]

            # Update the chunk meta because it can change even if the id doesn't
            if 'c' in chunk:
                chunk_meta['name'] = chunk['c']

            if 'l' in chunk:
                chunk_meta['labels'] = chunk['l'].copy()

            if 'a' in chunk:
                chunk_meta['attributes'] = chunk['a'].copy()

            identifier = chunk_meta['name'] + self.convert_dict_to_string(chunk_meta['labels'])
            attributes = self.convert_dict_to_string(chunk_meta['attributes'])
            for reading in chunk.get('v', []):
                reading_length = len(reading)
                row = {
                    'timestamp': reading[0],
                    'value': reading[-1],
                    'identifier': identifier,
                    'attributes': attributes
                }
                if reading_length == 3:
                    row['elevation'] = reading[1]
                elif reading_length == 4:
                    row['latitude'] = reading[1]
                    row['longitude'] = reading[2]
                elif reading_length == 5:
                    row['latitude'] = reading[1]
                    row['longitude'] = reading[2]
                    row['elevation'] = reading[3]
                yield row

    def _convert_row_to_gts_string(self, schema, row):
        gts_builder = GTSStringBuilder()
        for (column, value) in zip(schema['columns'], row):
            name = column['name']
            if name == 'timestamp':
                gts_builder.with_timestamp(value)
            elif name == 'latitude':
                gts_builder.with_latitude(value)
            elif name == 'longitude':
                gts_builder.with_longitude(value)
            elif name == 'elevation':
                gts_builder.with_elevation(value)
            elif name == 'identifier':
                gts_builder.with_identifier(value)
            elif name == 'value':
                gts_builder.with_value(value, column['type'])
        return gts_builder.to_string()

    def write_rows(self, schema, rows):
        logger.info('Writing {} rows to Warp10'.format(len(rows)))
        payload = '\n'.join(map(lambda row: self._convert_row_to_gts_string(schema, row), rows))
        self._update(payload)

    def exec_warpscript(self, warpscript):
        headers = {'Content-Type': 'text/plain; charset=UTF-8'}
        logger.info('Posting WarpScript to Warp10 for exec: {}'.format(warpscript))
        response = requests.post(self.base_url + 'exec', data=warpscript, headers=headers, verify=self.verify_ssl_cert)
        if not response.ok:
            if 'X-Warp10-Error-Line' in response.headers or 'X-Warp10-Error-Message' in response.headers:
                error_line = response.headers.get('X-Warp10-Error-Line', 'Unknown')
                error_message = response.headers.get('X-Warp10-Error-Message', 'Unknown')
                raise WarpScriptExecutionError('Error on WarpScript line {}: {}'.format(error_line, error_message))
            else:
                response.raise_for_status()
        return response.text
