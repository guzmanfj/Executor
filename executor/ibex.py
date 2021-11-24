"""
Module to run programs on ibex
"""

import numpy as np
import logging
from .executor import Executor
import re

from pathlib import Path

class RunError(Exception):
    """
    Class for exceptions
    """
    pass


class IbexRun(Executor):
    """
    Class to create jobs to run in ibex. When the `run()` method is called,
    a shell script will be submitted to the cluster via the `sbatch` command.

    Methods to overwrite:
    - `__init__`    set up your own inputs, and the output directory for the jobs
                    and the ibex stdout
    - `prepare`     create and save the script that will be submitted to sbatch.
                    In this script you can run terminal commands directly, or
                    other python scripts.

    """

    def __init__(self, time_per_command:int, out_ibex:Path, ncommands:int=1,
        jobname:str='IbexRun', partition:str='batch', ntasks:int=1,
        cpus_per_task:int=1, mem_per_cpu:int=2, max_jobs:int=1990, **kw):
        """
        Define variables for the ibex job

        Args:
            time_per_command (int):
                Time that each individual command takes to run.
            out_ibex (Path):
                Directory to save output from ibex stdout
            ncommands (int, optional): 
                Number of total commands to be run, which will later will be
                distributed in a maximum of 2,000 jobs in a job array.
                Defaults to 1.
            jobname (str, optional):
                Name of the job to be submitted. It will show in the ibex queue.
                Defaults to 'IbexRun'.
            partition (str, optional):
                Partition to run the job in ibex. Defaults to 'batch'.
            ntasks (int, optional):
                Number of tasks for the ibex job array. In most cases you will
                leave it as is. Defaults to 1.
            cpus_per_task (int, optional):
                Number of CPUs to be used per task. Defaults to 1.
            mem_per_cpu (int, optional):
                GBs of memory per CPU to request. Defaults to 2.
        """
        self.time_per_command = time_per_command
        self.jobname = jobname
        self.partition = partition
        self.out_ibex = out_ibex
        self.ntasks = ntasks
        self.cpus_per_task = cpus_per_task
        self.mem_per_cpu = mem_per_cpu
        self.ncommands=ncommands
        self.max_jobs = max_jobs

        self.commands_per_job = int(np.ceil( self.ncommands / self.max_jobs ))
        self.njobs = int(np.ceil( self.ncommands / self.commands_per_job ))

        self.time_per_job = self.time_str(self.commands_per_job,
            self.time_per_command)

        self.script_file = out_ibex / 'script.sh'

        self.args = f'sbatch {self.script_file}'.split()

        super().__init__(self.args, **kw)


    @staticmethod
    def time_str(commands_per_job:int, t_per_command:int) -> str:
        """
        Get the time of the job in the format hh:mm:ss according to the number
        of commands to run

        Args:
            commands_per_job (int): Number of commands to run per job in the array
            t_per_command (int): Time in minutes to run each command

        Returns:
            str: Time to request for each job in the array in hh:mm:ss format
        """
        total_minutes = commands_per_job * t_per_command
        hours = np.floor(total_minutes/60)
        minutes = np.ceil(total_minutes % 60)

        return f'{int(hours):02}:{int(minutes):02}:00'

    def prepare(self):
        """
        Make the script to be run in sbatch.
        This method will be overwritten in your own class, to make the script
        specific to the command that you wish to run.
        """
        if not self.out_ibex.exists():
            self.out_ibex.mkdir(parents=True)

        self.script = (
            # This part will stay the same:
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self.jobname}\n"
            f"#SBATCH --partition={self.partition}\n"
            f"#SBATCH --output={self.out_ibex}/%J.out\n"
            f"#SBATCH --time={self.time_per_job}\n"
            f"#SBATCH --ntasks={self.ntasks}\n"
            f"#SBATCH --cpus-per-task={self.cpus_per_task}\n"
            f"#SBATCH --mem-per-cpu={self.mem_per_cpu}G\n"
            f"#SBATCH --array=0-{self.njobs-1}\n"
            "\n"
            # This part will be overwritten:
            "echo 'Hello world!'\n"

            # Example using the SLURM_ARRAY_TASK_ID variable
            # "seq_file='sequences_${SLURM_ARRAY_TASK_ID}.fasta'\n"
            # "python script.py ${seq_file}\n"
        )

        logging.info(f'Script to be submitted:\n{self.script}')

        with open(self.script_file, 'w') as f:
            f.write(self.script)


    def finish(self) -> str:
        """
        Returns the job id if the submission to sbatch was successful.
        """
        return re.search(r'\d+', self.completed_process.stdout).group()
