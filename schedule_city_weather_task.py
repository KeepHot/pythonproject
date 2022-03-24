from crontask.cron_task_common import CronTaskInDolphinScheduler

url = "https://local-ci-smartapi.vesync.com/cloud/v1/inner/openCronService/cityWeatherScheduleTask"
method = "cityWeatherScheduleTask"
step = 60 * 60

cityWeatherScheduleTask = CronTaskInDolphinScheduler(url_appserver_cron_task=url, method=method, step=step)
cityWeatherScheduleTask.schedule_common_cron_task()
