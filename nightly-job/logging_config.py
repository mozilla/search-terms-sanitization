import logging.config
import dockerflow.logging

def configure_logging():
    cfg = {
        'version': 1,
        'formatters': {
            'json': {
                '()': dockerflow.logging.JsonLogFormatter,
                'logger_name': 'sanitation_job'
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'json'
            },
        },
        'loggers': {
            'sanitation_job': {
                'handlers': ['console'],
                'level': 'DEBUG',
            },
        }
    }

    logging.config.dictConfig(cfg)
