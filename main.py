import luther
import modeling

from loguru import logger


logger.level("RESULTS", no=40, color="<green>")
_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success_{_log_file_name}.log", level='SUCCESS')
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")

def main():
    """Run the entire pipeline.
    """
    logger.info(f"Starting the Pipeline")
    training, validation = luther.run_all()
    logger.success(f"Finished Getting Data")
    logger.info(f"Start Modeling and Validating")
    modeling.validate_all(training, validation)
    logger.success(f"Finished Modeling and Validating")
    logger.success(f"Finished Pipeline")