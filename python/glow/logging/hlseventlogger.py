from pyspark import SparkContext
from typing import Any, Dict


def record_hls_event(tag: str, arg_map: Dict[str, Any] = None, **kwargs: Any):
    """
    Logs the event
    Args:
        tag: Name of the tag
        arg_map: A string -> any map of options to go into blob
        kwargs: Named arguments. If the arg_map is not specified, logger options will be
          pulled from these keyword args.
    Returns:
    """

    logger_fn = SparkContext._jvm.io.projectglow.common.logging.PythonHlsEventRecorder.recordHlsEvent
    args = arg_map if arg_map is not None else kwargs
    logger_fn(tag, args)
    return
