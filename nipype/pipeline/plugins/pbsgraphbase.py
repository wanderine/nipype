"""Parallel workflow execution via SGE
"""

import os
import sys

from .base import (GraphPluginBase, logger)

from ...interfaces.base import CommandLine


class PBSGraphBasePlugin(GraphPluginBase):
    """Provides a common abstract base class for PBS graph based submissions with
       either PBS, Torque, SGE, or other similar systems to derive from

    The plugin_args input to run can be used to control the SGE execution.
    Currently supported options are:

    - template : template to use for batch job submission
    - qsub_args : arguments to be prepended to the job execution script in the
                  qsub call

    Subclasses that derive from this must provide
    instances of:
    _GetTemplate          <- To provide the correct job template script header for this type of queue system
    _GetHoldJobFlag       <- To provide the qsub command needed to identify job dependencies
    _GetJobIDParserString <- To provide the base command line needed to extract the unique job id from the returned string of qsub
    """

    ### Specialization for SGE
    def _GetTemplate(self):
        raise "FAILURE: Should never be called from abstract base class"

    def _GetHoldJobFlag():
        raise "FAILURE: Should never be called from abstract base class"

    def _GetJobIDParserString():
        raise "FAILURE: Should never be called from abstract base class"

    ### Common part for both SGE and the derived PBS class  It would be better to move this to a parent class
    ### That both SGE and PBS graph derive from
    def __init__(self, **kwargs):
        self._qsub_args = ''
        self._tempalte= self._GetTemplate()
        if 'plugin_args' in kwargs:
            plugin_args = kwargs['plugin_args']
            if 'template' in plugin_args:
                self._template = plugin_args['template']
                if os.path.isfile(self._template):
                    self._template = open(self._template).read()
            if 'qsub_args' in plugin_args:
                self._qsub_args = plugin_args['qsub_args']
        super(PBSGraphBasePlugin, self).__init__(**kwargs)

    def _submit_graph(self, pyfiles, dependencies, nodes):
        batch_dir, _ = os.path.split(pyfiles[0])
        submitjobsfile = os.path.join(batch_dir, 'submit_jobs.sh')
        with open(submitjobsfile, 'wt') as fp:
            fp.writelines('#!/usr/bin/env bash\n')
            for idx, pyscript in enumerate(pyfiles):
                node = nodes[idx]
                template, qsub_args = self._get_args(
                    node, ["template", "qsub_args"])

                batch_dir, name = os.path.split(pyscript)
                name = '.'.join(name.split('.')[:-1])
                batchscript = '\n'.join((template,
                                         '%s %s' % (sys.executable, pyscript)))
                batchscriptfile = os.path.join(batch_dir,
                                               'batchscript_%s.sh' % name)
                with open(batchscriptfile, 'wt') as batchfp:
                    batchfp.writelines(batchscript)
                    batchfp.close()
                deps = ''
                if idx in dependencies:
                    values = ['${job%05d}' %
                              jobid for jobid in dependencies[idx]]
                    if len(values):  # i.e. if some jobs were added to dependency list
                        deps = '%s %s' % (self._GetHoldJobFlag(), ','.join(values))
                jobname = 'job%05d' % (idx)
                ## Do not use default output locations if they are set in self._qsub_args
                batchscripterrfile = batchscriptfile + '.e'
                stderrFile = ''
                if self._qsub_args.count('-e ') == 0:
                        stderrFile = '-e {errFile}'.format(
                            errFile=batchscripterrfile)

                batchscriptoutfile = batchscriptfile + '.o'
                stdoutFile = ''

                if self._qsub_args.count('-o ') == 0:
                        stdoutFile = '-o {outFile}'.format(
                            outFile=batchscriptoutfile)

                full_line = '{jobNm}=$(qsub {outFileOption} {errFileOption} {extraQSubArgs} {dependantIndex} -N {jobNm} {batchscript} {jobIDParserString} )\n'.format(
                    jobNm=jobname,
                    outFileOption=stdoutFile,
                    errFileOption=stderrFile,
                    extraQSubArgs=qsub_args,
                    dependantIndex=deps,
                    batchscript=batchscriptfile,
                    jobIDParserString=self._GetJobIDParserString()
                    )
                fp.writelines(full_line)

        cmd = CommandLine('bash', environ=os.environ.data,
                          terminal_output='allatonce')
        cmd.inputs.args = '%s' % submitjobsfile
        cmd.run()
        logger.info('submitted all jobs to queue')
