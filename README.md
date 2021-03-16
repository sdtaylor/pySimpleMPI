# pySimpleMPI
Functions for running embarrassingly simple parallel tasks with MPI in python

**Hi, don't use this package. MPI is outdated, especially for python. Use [dask](https://dask.org/) instead.**

## Installation
This uses `mpi4py` which you can get from most channels.  

This is not on any python installation channel, so you must install from Github  

```
pip install git+git://github.com/sdtaylor/pySimpleMPI
```

## Usage  

Two classes are needed for this. One is a "boss" class which organizes a list of jobs, provides info on those jobs, and does any post processing of job results. The second is the worker class, which waits and runs jobs which are given to it.  
See the `examples/minimum_example.py` file for the exact details of how the classes should be structured. All class methods are required, even if they don't do anything. For example if the worker class doesn't require any setup, then define that method with `pass` as the single action.  
