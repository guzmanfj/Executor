import subprocess
import os
from subprocess import CalledProcessError
import tempfile
import shutil
import time
import logging

from pathlib import Path

class RunError(Exception):
    """
    Base class for exceptions
    """
    pass


class Executor:
    """
    Class to make calls to external programs

    Create a subclass of Executor for running your programs. The methods that
    you may want to override are:
    
    - `__init__` ... to set your own default values (might want to call parent __init__ too)
    - `prepare` ... this one is called BEFORE program execution, best used to prepare input files
    - `finish` ... called AFTER the successful program execution, to process output as needed
    - `isFailed` ... for situations where the program ran successfully (exited with code 0), but there is still some unwanted output

    Once you instantiate the class with the appropriate arguments, you can run
    the program with the run method. Example to run the 'ls' command in the
    current directory:

        >>> exe = Executor(['ls', '.'], catch_out=False)
        >>> args, returncode, stdout = exe.run() 
    """

    def __init__(self, args:list, catch_out:bool=True, dir_out:Path=None,
        tempdir:Path=None, keep_tempdir:bool=False, cwd:Path=Path.cwd(),
        verbose:bool=True):
        """
        Initialize the necessary variables to run the program. Override, but
        call parent method with super().__init__(...)

        Args:
            args (list): 
                List with the command name and arguments, as it would be run
                in the command line. Only a single program can be run. If you
                need to redirect the output somewhere, take the 
                self.completed_process.stdout variable and use that accordingly
            catch_out (bool or str, optional): 
                Whether or not to catch the stdout from the program. If True,
                creates an output file called {program}.out in the cwd and saves
                stdout to that file. If a string is provided, it will be used
                as a file name for the output file. Defaults to True.
            dir_out (Path, optional):
                Directory to save the outputs of the program. Defaults to None.
            tempdir (Path, optional): 
                Directory to create temporary input/output files.
                Defaults to None.
            keep_tempdir (bool, optional): 
                Whether or not to keep the temporary folder. If False, it will
                be deleted in the cleanup() method. Defaults to False.
            cwd (str, optional): Working directory to run the program in, and
                to save the stdout if catch_out=True. Defaults to '.'.
            verbose (bool, optional): Prints progress messages and error message
                from fail() to the terminal. Defaults to True.
        """
        self.program = args[0]
        self.args = args
        self.catch_out = catch_out
        self.keep_tempdir = keep_tempdir
        self.cwd = cwd
        self.tempdir = tempdir
        self.verbose = verbose
        self.dir_out = dir_out

        if self.verbose:
            logging.basicConfig(format='%(levelname)s:%(message)s',
                level=logging.INFO)

        if catch_out == True:
            self.f_stdout = self.cwd / f'{self.program}.out'
        elif isinstance(catch_out, str):
            self.f_stdout = self.cwd / catch_out

    def prepare(self):
        """
        Prepare input for program (e.g. create input files).
        OVERRIDE, especially if you don't need to create a temporary directory
        for your input/output
        """
        logging.info('Creating temporary directory...')
        if self.tempdir:
            if not self.tempdir.exists():
                self.tempdir.mkdir()
        else:
            self.tempdir = Path(tempfile.mkdtemp(
                prefix=self.program+'_'+self.__class__.__name__.lower()+'_'))

        if self.dir_out:
            if not self.dir_out.exists():
                logging.info('Creating output directory...')
                self.dir_out.mkdir(parents=True)


    def execute(self):
        """
        Run external command and block until it is finished
        """
        start_time = time.time()
        
        p = subprocess.run(self.args, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, cwd=self.cwd)
        
        runtime = time.time() - start_time

        return runtime, p


    def run(self):
        """
        Method that will be called for running the external program
        """
        self.completed_process = None
        self.failed_message = None
        try:
            self.prepare()

            logging.info(f'Running command:\n$ {" ".join(self.args)}')
            self.runtime, self.completed_process = self.execute()

            # Check if exit code of process was 0
            self.completed_process.check_returncode()

            # Check for other errors/not desired output even if exit code is 0
            self.failed_message = self.isFailed()
            if self.failed_message:
                self.error = RunError
                self.fail()
            else:
                logging.info('Command was run successfully.')
                logging.info(f'stdout:\n{self.completed_process.stdout}')
                self.result = self.finish()

        except OSError as e:
            raise RunError(
                f"Couldn't run or communicate with external program: {e.strerror}")

        except (MemoryError, CalledProcessError) as e:
            self.error = e
            self.fail()

        finally:
            self.cleanup()
        
        return self.result


    def fail(self):
        """
        Generates the error message and raises the corresponding error if the
        program fails.
        """
        error_string = \
            f"\n{self.program} EXECUTION FAILED.\n"+\
            f"Command: {' '.join(self.args)}\n"
        if self.completed_process:
            error_string += \
                f"Returncode: {self.completed_process.returncode}\n"+\
                f"stdout: \n"+\
                self.completed_process.stdout
        
        if self.verbose:
            logging.error(error_string)

        if self.catch_out:
            with open(self.f_stdout, 'w') as f:
                f.write(error_string)

        if self.failed_message:
            raise self.error(self.failed_message)
        else:
            raise self.error


    def isFailed(self):
        """
        Not needed in most cases. Only override when the program may produce
        unwanted output even if it runs successfully (the exit code is 0). 
        Check for the output and return your own error message, be
        as descriptive as you wish.
        """
        undesired_output_present = False
        if undesired_output_present:
            return "The output is not as expected."
        else:
            return False


    def finish(self):
        """
        OVERRIDE to process output as necessary, e.g. save to output file or
        parse into pandas DataFrame.
        Be sure to save output in a location other than the temporary directory,
        which will be removed in the cleanup() method.
        """
        if self.catch_out:
            with open(self.f_stdout, 'w') as f:
                f.write(self.completed_process.stdout)

        result = (
                self.completed_process.args,
                self.completed_process.returncode,
                self.completed_process.stdout
        )

        return result


    def cleanup(self):
        """
        Remove temporary directory after external program has finished
        (failed or not)
        """
        if self.tempdir and self.tempdir.exists():
            if not self.keep_tempdir:
                logging.info('Removing temporary directory...')
                shutil.rmtree(self.tempdir)
