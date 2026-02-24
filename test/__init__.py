# Set default logging handler to avoid "No handler found" warnings.
import logging
logging.basicConfig(level=logging.INFO)

import os
log_file_path = os.environ.get('KAFKA_PYTHON_PROTOCOL_DEBUG_LOG')
if log_file_path:
    # Get the specific logger for kafka.protocol.parser
    protocol_parser_logger = logging.getLogger('kafka.protocol.parser')
    protocol_parser_logger.setLevel(logging.DEBUG) # Set its level to DEBUG

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG) # Set the handler's level to DEBUG

    # Create a formatter for the log messages
    formatter = logging.Formatter('%(thread)d %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    protocol_parser_logger.addHandler(file_handler)

    # Optional: Prevent propagation to the root logger to avoid duplicate console output
    protocol_parser_logger.propagate = False


from kafka.future import Future
Future.error_on_callbacks = True  # always fail during testing
