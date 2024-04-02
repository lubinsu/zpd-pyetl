# @Time : 03/28/2024 09:14 AM
# @Author : lubinsu
# @File : CronTab.py
# @desc : 定时任务实体类

class CronTab:

    def __init__(self, cron, jobs_name, cron_type):

        self.cron = cron
        self.jobs_name = jobs_name
        self.cron_type = cron_type
