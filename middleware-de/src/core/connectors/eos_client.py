# [TASK 28, 30, 32, 34, 36, 38] Client API EOS
# Code client gọi API EOS lấy dữ liệu Organization, User, Project, Task, Work Result, Performance
import requests
from config import settings

class EOSClient:
    def __init__(self, base_url=settings.EOS_API_URL):
        self.base_url = base_url
        self.session = requests.Session()

    def get_organizations(self, page=1, limit=100, last_sync_at=None):
        # [TASK 28] Pull API sync Organization từ EOS
        pass

    def get_users(self, page=1, limit=100, last_sync_at=None):
        # [TASK 30] Pull API sync User/Employee từ EOS
        pass

    def get_projects(self, page=1, limit=100, last_sync_at=None):
        # [TASK 32] Pull API sync Project từ EOS
        pass

    def get_tasks(self, page=1, limit=100, last_sync_at=None):
        # [TASK 34] Pull API sync Task từ EOS
        pass

    def get_work_results(self, page=1, limit=100, last_sync_at=None):
        # [TASK 36] Pull API sync Work Result từ EOS
        pass

    def get_performances(self, page=1, limit=100, last_sync_at=None):
        # [TASK 38] Pull API sync Performance từ EOS
        pass
