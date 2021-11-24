import unittest

import os, sys, shutil, tempfile
from pathlib import Path

sys.path.append(os.fspath(Path(__file__).parent.parent))

from executor.ibex import IbexRun

class IbexTest(unittest.TestCase):
    """
    Class for testing the Ibex module
    """

    def setUp(self) -> None:
        self.time_per_command = 1
        self.jobname = 'ibex_unittest'
        self.partition = 'debug'

        self.tempdir = Path(tempfile.gettempdir(),
            self.__class__.__name__.lower())
        
        self.out_ibex = self.tempdir
        self.script_file = self.out_ibex / 'script.sh'
        
        self.ibex = IbexRun(time_per_command=self.time_per_command,
            out_ibex=self.out_ibex, jobname=self.jobname,
            partition=self.partition, verbose=False)
    
    def test_command_created(self) -> None:
        """
        Test that the correct command for sbatch was created
        """
        self.assertEqual(self.ibex.args, f'sbatch {self.script_file}'.split())

    def test_script_created(self) -> None:
        """Test that the script file is created successfully.
        """
        self.ibex.prepare()

        self.assertTrue(self.script_file.exists())

        with open(self.script_file, 'r') as f:
            contents = f.read()

        expected_contents = (
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self.jobname}\n"
            f"#SBATCH --partition=debug\n"
            f"#SBATCH --output={self.out_ibex}/%J.out\n"
            f"#SBATCH --time=00:01:00\n"
            f"#SBATCH --ntasks=1\n"
            f"#SBATCH --cpus-per-task=1\n"
            f"#SBATCH --mem-per-cpu=2G\n"
            f"#SBATCH --array=0-0\n"
            "\n"
            "echo 'Hello world!'\n"
        )

        self.assertEqual(contents, expected_contents)

    # def test_job_submitted(self) -> None:
    #     """
    #     Test that the job is submited to ibex successfully. Only to test in ibex.
    #     """

    #     jobid = self.ibex.run()

    #     self.assertTrue(jobid.isdigit())
    #     self.assertTrue(len(jobid)==8)

    def tearDown(self):
        if self.ibex.out_ibex.exists():
            shutil.rmtree(self.ibex.out_ibex)

if __name__ == '__main__':
    unittest.main()
