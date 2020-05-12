import logging
import os.path
from datetime import datetime

import dataiku
from dataiku.customrecipe import get_recipe_config, get_output_names_for_role, get_output_names
from warp10 import Warp10Client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='Warp10 recipe %(levelname)s - %(message)s')

recipe_config = get_recipe_config()

warp10_connection = recipe_config.get('warp10_connection', None)
warpscript = recipe_config.get('code', None)

if not warp10_connection:
    raise ValueError('No Warp10 connection defined')

if not warpscript:
    raise ValueError('No WarpScript code entered')

warp10_client = Warp10Client(warp10_connection)

logger.info('Appending UPDATE function to end of WarpScript code')
warpscript = warpscript + "\n'{}' UPDATE".format(warp10_connection['write_token'])

result = warp10_client.exec_warpscript(warpscript)

have_folder = get_output_names_for_role('main_output')
if have_folder:
    # Semi-dummy output since there is really nothing to do at this point
    output_folder_name = get_output_names_for_role('main_output')[0]
    output_folder = dataiku.Folder(output_folder_name)

    filename = 'Run_{}.txt'.format(datetime.now().strftime('%Y-%m-%dT%H-%M-%S-%f')[:-3])
    logger.info('Writing response file {} in output folder', filename)
    with open(os.path.join(output_folder.get_path(), filename), 'w') as results_file:
        results_file.write('Response of successful WarpScript execution:\n' + result)
