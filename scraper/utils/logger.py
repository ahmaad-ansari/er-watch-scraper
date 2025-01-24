import logging
import colorlog

def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a color-coded logger using the colorlog package.

    This function sets up a logger with a custom formatter that color-codes
    messages based on their severity levels. It ensures that multiple handlers
    are not added to the same logger if called repeatedly.

    Args:
        name (str): The name of the logger, typically passed as __name__.

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Retrieve or create a logger by name.
    logger = logging.getLogger(name)

    # If this logger already has handlers, return it as-is to avoid duplicates.
    if logger.hasHandlers():
        return logger

    # Set the default logging level. Messages at or above this level are shown.
    logger.setLevel(logging.DEBUG)

    # Create a colorlog formatter with a pattern and color mapping for each log level.
    # The format includes:
    #   - %(log_color)s: The color escape codes provided by colorlog
    #   - %(levelname)s: The level of the message (DEBUG, INFO, etc.)
    #   - %(name)s: The logger's name
    #   - %(lineno)d: The line number where the logger was called
    #   - %(message)s: The actual log message
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s[%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        log_colors={
            "DEBUG":    "light_black",
            "INFO":     "white",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        }
    )

    # Create a handler that outputs log messages to the console (stdout).
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Attach the console handler to the logger.
    logger.addHandler(console_handler)

    return logger
