import uuid

class IdGenerator:
    """
    Utility class responsible for generating unique identifiers across the pipeline.
    Implemented as a static class to avoid unnecessary instantiation.
    """
    
    @staticmethod
    def generate() -> str:
        """
        Generates a standard UUID4 string to serve as correlation or local transaction IDs.
        """
        return str(uuid.uuid4())