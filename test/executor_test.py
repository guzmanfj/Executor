import unittest
import os, sys, tempfile, shutil
from subprocess import CalledProcessError
from pathlib import Path

sys.path.append(os.fspath(Path(__file__).parent.parent))

from executor.executor import Executor
from executor.executor import RunError

class ExecutorTest(unittest.TestCase):
    """
    Class for testing the executor module
    """

    def setUp(self):
        self.args = ['ls', '.']
        self.program = self.args[0]
        self.tempdir = Path(tempfile.gettempdir(),
            'ls_'+self.__class__.__name__.lower())
        self.cwd = self.tempdir

    def test_createsTempdir(self):
        """
        Creates a temp directory when no tempdir is specified
        """
        self.exe = Executor(self.args, catch_out=False,
            tempdir=None, keep_tempdir=True, verbose=False)
        self.exe.run()
        
        self.assertTrue(self.exe.tempdir.exists())

    def test_createsSpecifiedTempdir(self):
        """
        Creates the specified tempdir
        """
        self.assertFalse(self.tempdir.exists())

        self.exe = Executor(self.args, catch_out=False,
            tempdir=self.tempdir, keep_tempdir=True, verbose=False)
        self.exe.run()
        
        self.assertTrue(self.tempdir.exists())

    def test_createsSpecifiedOutDir(self):
        """
        Creates the specified tempdir
        """
        dir_out = self.tempdir / 'output'
        self.assertFalse(dir_out.exists())

        self.exe = Executor(self.args, catch_out=False, dir_out=dir_out,
            tempdir=self.tempdir, keep_tempdir=True, verbose=False)
        self.exe.run()
        
        self.assertTrue(dir_out.exists())

    def test_removesTempdir(self):
        """
        Removes tempdir after execution
        """
        self.exe = Executor(self.args, catch_out=False,
            tempdir=None, keep_tempdir=False, verbose=False)
        self.exe.run()

        self.assertFalse(self.exe.tempdir.exists())

    def test_invalid_program(self):
        """
        Raises RunError when called with an invalid program
        """
        args = ['l', '.']
        self.exe = Executor(args, catch_out=False,
            tempdir=None, keep_tempdir=False, verbose=False)
        
        with self.assertRaises(RunError):
            self.exe.run()

    def test_invalid_arguments(self):
        """
        Raises CalledProcessError when called with invalid arguments and
        exit code is not zero
        """
        args = ['ls', 'hello']
        self.exe = Executor(args, catch_out=False, tempdir=None,
            keep_tempdir=False, verbose=False)

        with self.assertRaises(CalledProcessError):
            self.exe.run()

    def test_custom_error_message(self):
        """
        Raises RunError with custom error message if provided in isFailed()
        """
        error_message="The output is not as expected"
        
        class FailingProgram(Executor):
            def isFailed(self):
                return error_message
        
        self.exe = FailingProgram(self.args, catch_out=False, tempdir=None,
            keep_tempdir=False, verbose=False)

        with self.assertRaises(RunError) as cm:
            self.exe.run()

        exception = cm.exception
        self.assertEqual(exception.args[0], error_message)

    def test_ls_output(self):
        """
        Test the output of the `ls` program
        """
        self.tempdir.mkdir()
        open(Path(self.tempdir, 'test1.txt'), 'w').close()
        open(Path(self.tempdir, 'test2.txt'), 'w').close()
        open(Path(self.tempdir, 'test3.txt'), 'w').close()

        self.exe = Executor(self.args, catch_out=False, tempdir=self.tempdir,
            keep_tempdir=True, cwd=self.cwd, verbose=False)
        self.out = self.exe.run()

        self.assertEqual(len(self.out), 3)
        self.assertEqual(self.out[0], ['ls', '.'])
        self.assertEqual(self.out[1], 0)
        self.assertEqual(self.out[2], 'test1.txt\ntest2.txt\ntest3.txt\n')

    def test_catch_ls_output(self):
        """
        Catch the output of the 'ls' program correctly
        """
        self.tempdir.mkdir()
        open(Path(self.tempdir, 'test1.txt'), 'w').close()
        open(Path(self.tempdir, 'test2.txt'), 'w').close()
        open(Path(self.tempdir, 'test3.txt'), 'w').close()
        
        self.exe = Executor(self.args, catch_out=True, tempdir=self.tempdir,
            keep_tempdir=True, cwd=self.cwd, verbose=False)
        self.out = self.exe.run()

        f_out = Path(self.cwd, f'{self.program}.out')
        self.assertTrue(os.path.isfile(f_out))

        with open(f_out, 'r') as f:
            contents = f.read()
        self.assertEqual(contents, 'test1.txt\ntest2.txt\ntest3.txt\n')

    def tearDown(self):
        if self.exe.tempdir.exists():
            shutil.rmtree(self.exe.tempdir)


if __name__ == '__main__':
    unittest.main()
