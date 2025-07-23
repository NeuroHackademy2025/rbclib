# rbclib

A library for programmatically accessing the RBC Database during the NeuroHackademy.

This library contains a type `RBCPath` that inherits from the `CloudPath` type of
[`cloudpathlib`](https://cloudpathlib.drivendata.org/stable/).  It allows for
programmatic access to the [RBC Database](https://reprobrainchart.github.io/).

Note that this library is intended for use during the 2025
[NeuroHackademy](https://neurohackademy.org/). It expects that certain files will
already exist in particular directories (that are available on the NeuroHackademy
JupyterHub). The library should still work outside of the NeuroHackademy, but it
will likely generate rate limit errors from GitHub if used heavily outside of the
NeuroHackademy JupyterHub.


