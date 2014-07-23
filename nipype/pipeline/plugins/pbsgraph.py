"""Parallel workflow execution via PBS/Torque
"""

import os
import sys

from .base import (GraphPluginBase, logger)

from ...interfaces.base import CommandLine
from .pbsgraphbase import PBSGraphBasePlugin


class PBSGraphPlugin(PBSGraphBasePlugin):
    """Execute using PBS/Torque

    The plugin_args input to run can be used to control the SGE execution.
    Currently supported options are:

    - template : template to use for batch job submission
    - qsub_args : arguments to be prepended to the job execution script in the
                  qsub call

    """

    ### Specialization for PBS
    def _GetTemplate(self):
        batch_script_template = """#!/bin/bash
#PBS -V
"""
        return batch_script_template

    def _GetHoldJobFlag():
        return "-W depend=afterok:"

    def _GetJobIDParserString():
        return "| awk \'{{print $1}}\'"  ##First item returned is the jobID
