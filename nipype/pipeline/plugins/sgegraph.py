"""Parallel workflow execution via SGE
"""

import os
import sys

from .base import (GraphPluginBase, logger)

from ...interfaces.base import CommandLine
from .pbsgraphbase import PBSGraphBasePlugin


class SGEGraphPlugin(PBSGraphBasePlugin):
    """Execute using SGE

    The plugin_args input to run can be used to control the SGE execution.
    Currently supported options are:

    - template : template to use for batch job submission
    - qsub_args : arguments to be prepended to the job execution script in the
                  qsub call

    """

    ### Specialization for SGE
    def _GetTemplate(self):
        batch_script_template = """#!/bin/bash
#$ -V
#$ -S /bin/bash
"""
        return batch_script_template

    def _GetHoldJobFlag():
        return "-hold_jid "

    def _GetJobIDParserString():
        return "| awk \'{{print $3}}\'"  ##Third item returned is the jobID
