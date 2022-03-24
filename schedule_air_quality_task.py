from crontask.cron_task_common import CronTaskInDolphinScheduler

url = "https://local-ci-smartapi.vesync.com/cloud/v1/inner/openCronService/airQualityScheduleTask"
method = "airQualityScheduleTask"
step = 60 * 60 * 4

airQualityScheduleTask = CronTaskInDolphinScheduler(url_appserver_cron_task=url, method=method, step=step)
airQualityScheduleTask.schedule_common_cron_task()
