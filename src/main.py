# deletes records in tables as specified in config
# does not overwrite the table, so, it can be safely used when other components are writing in the same table

import os
import logging
import time

import requests
from keboola import docker
import logging_gelf.handlers
import logging_gelf.formatters


def read_time_value_from_file(source_file, datadir):
    """
    Reads first field from the second line in file.
    File needs to be comma-separated.
    """
    with open(f'{datadir}in/tables/{source_file}.csv') as f:
        # throw away the header
        f.readline()
        time_value = f.readline().split(',')[0]
    return time_value.replace('"', '')


def wait_for_job(token, job_url):
    """
    Waits for KBC job to return either success or fail.
    """
    headers = {
        'X-StorageApi-Token': token
    }
    while (status := requests.get(job_url, headers=headers).json()['status']) != 'success':
        if status != 'processing' and status != 'waiting':
            logging.error('Deleting job failed.')
            break
        time.sleep(1)
    logging.info(f'Job status: {status}')


def generate_config_for_table(table_dict, datadir):
    """
    Constructs deletion config for table. Uses source file if specified.
    If not, value from config is used.
    """
    if (time_value_source := table_dict.get('time_value_source')) is not None:
        time_value = read_time_value_from_file(time_value_source, datadir)
    else:
        time_value = table_dict['time_value']
    return {table_dict['time_method']: time_value}


def delete_table_rows(token, table_dict, datadir):
    """
    Deletes rows as specified in config.
    """

    params = generate_config_for_table(table_dict, datadir)

    logging.info(f'Running for table: {table_dict["table_id"]}')
    logging.info(f'Params: {params}')
    headers = {
        'X-StorageApi-Token': token
    }

    request = requests.delete(
        f'https://connection.eu-central-1.keboola.com/v2/storage/tables/{table_dict["table_id"]}/rows',
        headers=headers, data=params)

    job_url = request.json()['url']
    wait_for_job(token, job_url)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(host=os.getenv('KBC_LOGGER_ADDR'),
                                                                          port=int(os.getenv('KBC_LOGGER_PORT')))
        # remove stdout logging when running inside keboola
        logger.removeHandler(logger.handlers[0])
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    kbc_datadir = os.getenv('KBC_DATADIR', '/data/')
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()
    # log parameters (excluding sensitive designated by '#')
    logging.info({k: v for k, v in parameters.items() if "#" not in k})

    kbc_token = parameters['#token']

    for table in parameters["tables"]:
        delete_table_rows(kbc_token, table, kbc_datadir)
