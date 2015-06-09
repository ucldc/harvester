REDIS_HOST="{{ redis_host }}"
REDIS_PORT={{ redis_port }}
REDIS_CONNECT_TIMEOUT={{ redis_connect_timeout }}
QUEUES= [ 'high{{ name_suffix }}', 'normal{{ name_suffix }}',
          'low{{ name_suffix }}']
