from dataiku.runnables import Runnable
from warp10 import Warp10Client

class Warp10Exec(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config

    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        warp10_connection = self.config.get('warp10_connection', None)
        warpscript = self.config.get('code', None)

        if not warp10_connection:
            raise ValueError('No Warp10 connection defined')

        if not warpscript:
            raise ValueError('No WarpScript code entered')

        warp10_client = Warp10Client(warp10_connection)
        result = warp10_client.exec_warpscript(warpscript)
        return '<pre>' + result + '</pre>'
