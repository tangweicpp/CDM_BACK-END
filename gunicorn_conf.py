import multiprocessing

bind = '0.0.0.0:5025'
workers = multiprocessing.cpu_count() * 2 + 1

backlog = 2048
worker_class = "gevent"
worker_connections = 1000
daemon = False
debug = True
proc_name = 'gunicorn_cdm_back'
pidfile = './log/gunicorn.pid'
errorlog = './log/gunicorn.log'