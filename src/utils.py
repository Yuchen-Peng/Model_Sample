# -*- coding: utf-8 -*-
import logging
import os
import pandas as pd
import numpy as np

import coloredlogs


def create_logger(level: str = os.environ.get("LOGLEVEL", "DEBUG")) -> logging.Logger:
    """ """
    logger = logging.getLogger("efficient")

    coloredlogs.install(
        level=getattr(logging, level.upper()),
        logger=logger,
        fmt="%(asctime)s - [%(levelname)s %(filename)s:%(lineno)d] %(message)s",
    )
    return logger


log = create_logger()
