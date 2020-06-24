import logging

from properties import LOG_PATH

class LoggerSetup():
    def get_logger(self, ctrl_file):
            control_file = LOG_PATH + ctrl_file
            # control_file = LOG_PATH + 'file_mover_' + time.strftime("%Y%m%d-%H%M%S") + '.log'
            log = logging.getLogger('log')
            logging.basicConfig(
                format='[%(asctime)s][%(process)-6s][%(levelname)-8s]: %(message)s',
                level=logging.DEBUG,
                handlers=[
                    logging.FileHandler(control_file),
                    logging.StreamHandler()
                    ]
            )
            log.info('Logging has been set up. Control file: {}'.format(control_file))
            return log