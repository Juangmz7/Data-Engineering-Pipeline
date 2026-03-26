import logging

class PipelineLogFormatter(logging.Formatter):
    """
    Custom formatter enforcing the strict logging structure:
    Date time with exactly mins and seconds - Class name - correlationId - logInfo - localId
    """
    def format(self, record: logging.LogRecord) -> str:
        time_str = self.formatTime(record, '%Y-%m-%d %H:%M:%S')
        # We rely on the LoggerAdapter to inject class_name, correlation_id, and local_id into the record
        return (f"{time_str} - {record.class_name} - {record.correlation_id} - "
                f"{record.getMessage()} - {record.local_id}")

def get_pipeline_logger(class_name: str, correlation_id: str, local_id: str) -> logging.LoggerAdapter:
    """
    Creates and configures a LoggerAdapter that automatically injects pipeline context.
    """
    logger = logging.getLogger(f"{class_name}.{local_id}")
    
    # Prevent adding multiple handlers if the logger already exists in memory
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(PipelineLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False 

    extra = {
        'class_name': class_name,
        'correlation_id': correlation_id,
        'local_id': local_id
    }
    
    return logging.LoggerAdapter(logger, extra)