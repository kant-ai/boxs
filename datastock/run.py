"""Functions for managing the run id."""
import logging
import uuid


logger = logging.getLogger(__name__)


_RUN_ID = None


def get_run_id():
    """
    Returns the run id.

    The run id is a unique identifier that is specific to an individual run of a
    workflow. It stays the same across all task executions and can be used for
    tracking metrics and differentiating between different runs of the same workflow
    where task_id and run_id stay the same.

    Returns:
        str: The unique run id.
    """
    if _RUN_ID is None:
        set_run_id(str(uuid.uuid1()))
    return _RUN_ID


def set_run_id(run_id):
    """
    Sets the run id.

    Setting the run id explicitly is usually not necessary. The function is mainly
    used when task executions are run in a different process to make sure the run id
    is consistent with the spawning process, but it can be used e.g. if an external
    system provides a unique identifier for a specific workflow run.

    When `set_run_id(run_id)` is being used, it must be run before the first tasks
    are actually defined.

    Raises:
        RuntimeError: If the run id was already set before.
    """
    global _RUN_ID  # pylint: disable=global-statement
    if _RUN_ID is not None:
        logger.error("run_id already set to %s when trying to set again", _RUN_ID)
        raise RuntimeError("Run ID was already set")
    logger.info("Set run_id to %s", run_id)
    _RUN_ID = run_id
