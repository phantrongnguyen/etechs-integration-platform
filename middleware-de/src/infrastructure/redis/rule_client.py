import json
import logging
import redis
from src.core.config import settings

logger = logging.getLogger(__name__)

class RuleClient:
    """
    Client for interacting with Redis to fetch Mapping Rules.
    This fulfills the 'Rule Sync' mechanism decoupling middleware-be and middleware-de.
    """
    def __init__(self):
        try:
            self.client = redis.from_url(settings.REDIS_URL, decode_responses=True)
            logger.info(f"Connected to Redis at {settings.REDIS_URL}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.client = None

    def get_mapping_rules(self, source_system: str, object_type: str) -> dict:
        """
        Read Rule from Redis Cache as defined in Enterprise Architecture.
        Returns empty dict if no rules found or connection fails.
        """
        if not self.client:
            return {}
            
        key = f"mapping_rules:{source_system}:{object_type}"
        try:
            data = self.client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error reading rules from Redis for key {key}: {e}")
            
        return {}
